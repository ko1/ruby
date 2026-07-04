// this file is included by ractor.c

struct ractor_port {
    rb_ractor_t *r;
    st_data_t id_;
};

static st_data_t
ractor_port_id(const struct ractor_port *rp)
{
    return rp->id_;
}

static VALUE rb_cRactorPort;

static VALUE ractor_receive(rb_execution_context_t *ec, const struct ractor_port *rp);
static VALUE ractor_send(rb_execution_context_t *ec, const struct ractor_port *rp, VALUE obj, VALUE move);
static VALUE ractor_try_send(rb_execution_context_t *ec, const struct ractor_port *rp, VALUE obj, VALUE move);
static void ractor_add_port(rb_ractor_t *r, st_data_t id);

// RLGCv2 (design_v2.md §4.5): off-heap move courier, defined in ractor.c
struct rb_ractor_move_courier *ractor_move_courier_build(VALUE obj);
VALUE ractor_move_courier_materialize(struct rb_ractor_move_courier *c);
void ractor_move_courier_free(struct rb_ractor_move_courier *c);
void ractor_move_courier_mark(struct rb_ractor_move_courier *c);

static void
ractor_port_mark(void *ptr)
{
    const struct ractor_port *rp = (struct ractor_port *)ptr;

    if (rp->r) {
        rb_gc_mark(rp->r->pub.self);
    }
}

static const rb_data_type_t ractor_port_data_type = {
    "ractor/port",
    {
        ractor_port_mark,
        RUBY_TYPED_DEFAULT_FREE,
        NULL, // memsize
        NULL, // update
    },
    0, 0, RUBY_TYPED_THREAD_SAFE_FREE | RUBY_TYPED_WB_PROTECTED | RUBY_TYPED_FROZEN_SHAREABLE | RUBY_TYPED_EMBEDDABLE,
};

static st_data_t
ractor_genid_for_port(rb_ractor_t *cr)
{
    // TODO: enough?
    return cr->sync.next_port_id++;
}

static struct ractor_port *
RACTOR_PORT_PTR(VALUE self)
{
    VM_ASSERT(rb_typeddata_is_kind_of(self, &ractor_port_data_type));
    return RTYPEDDATA_GET_DATA(self);
}

static VALUE
ractor_port_alloc(VALUE klass)
{
    struct ractor_port *rp;
    VALUE rpv = TypedData_Make_Struct(klass, struct ractor_port, &ractor_port_data_type, rp);
    rb_obj_freeze(rpv);
    return rpv;
}

static VALUE
ractor_port_init(VALUE rpv, rb_ractor_t *r)
{
    struct ractor_port *rp = RACTOR_PORT_PTR(rpv);

    rp->r = r;
    RB_OBJ_WRITTEN(rpv, Qundef, r->pub.self);
    rp->id_ = ractor_genid_for_port(r);

    ractor_add_port(r, ractor_port_id(rp));

    rb_obj_freeze(rpv);

    return rpv;
}

/*
 *  call-seq:
 *    Ractor::Port.new  -> new_port
 *
 *  Returns a new Ractor::Port object.
 */
static VALUE
ractor_port_initialize(VALUE self)
{
    return ractor_port_init(self, GET_RACTOR());
}

/* :nodoc: */
static VALUE
ractor_port_initialize_copy(VALUE self, VALUE orig)
{
    struct ractor_port *dst = RACTOR_PORT_PTR(self);
    struct ractor_port *src = RACTOR_PORT_PTR(orig);
    dst->r = src->r;
    RB_OBJ_WRITTEN(self, Qundef, dst->r->pub.self);
    dst->id_ = ractor_port_id(src);

    return self;
}

static VALUE
ractor_port_new(rb_ractor_t *r)
{
    VALUE rpv = ractor_port_alloc(rb_cRactorPort);
    ractor_port_init(rpv, r);
    return rpv;
}

static bool
ractor_port_p(VALUE self)
{
    return rb_typeddata_is_kind_of(self, &ractor_port_data_type);
}

static VALUE
ractor_port_receive(rb_execution_context_t *ec, VALUE self)
{
    const struct ractor_port *rp = RACTOR_PORT_PTR(self);

    if (rp->r != rb_ec_ractor_ptr(ec)) {
        rb_raise(rb_eRactorError, "only allowed from the creator Ractor of this port");
    }

    VALUE v = ractor_receive(ec, rp);
    RB_GC_GUARD(self);
    return v;
}

static VALUE
ractor_port_send(rb_execution_context_t *ec, VALUE self, VALUE obj, VALUE move)
{
    const struct ractor_port *rp = RACTOR_PORT_PTR(self);
    ractor_send(ec, rp, obj, RTEST(move));
    RB_GC_GUARD(self);
    return self;
}

static bool ractor_closed_port_p(rb_execution_context_t *ec, rb_ractor_t *r, const struct ractor_port *rp);
static bool ractor_close_port(rb_execution_context_t *ec, rb_ractor_t *r, const struct ractor_port *rp);

static VALUE
ractor_port_closed_p(rb_execution_context_t *ec, VALUE self)
{
    const struct ractor_port *rp = RACTOR_PORT_PTR(self);
    rb_ractor_t *r = rp->r;
    bool closed;

    if (rb_ec_ractor_ptr(ec) == r) {
        /* The owner's threads are serialized by the ractor GVL, so the ports
         * table can't change under this lookup. */
        closed = ractor_closed_port_p(ec, r, rp);
    }
    else {
        /* A foreign Ractor races the owner's st_insert/st_delete on the ports
         * table; take the lock like every other foreign reader. ractor_closed_port_p
         * asserts the lock is held for foreign access, and Port#closed? was the
         * only path reaching it without the lock. */
        RACTOR_LOCK(r);
        {
            closed = ractor_closed_port_p(ec, r, rp);
        }
        RACTOR_UNLOCK(r);
    }

    return closed ? Qtrue : Qfalse;
}

static VALUE
ractor_port_close(rb_execution_context_t *ec, VALUE self)
{
    const struct ractor_port *rp = RACTOR_PORT_PTR(self);
    rb_ractor_t *cr = rb_ec_ractor_ptr(ec);

    if (cr != rp->r) {
        rb_raise(rb_eRactorError, "closing port by other ractors is not allowed");
    }

    ractor_close_port(ec, cr, rp);
    return self;
}

// ractor-internal

// ractor-internal - ractor_basket

enum ractor_basket_type {
    // basket is empty
    basket_type_none,

    // value is available
    basket_type_ref,
    basket_type_copy,
    basket_type_move,
};

struct ractor_basket {
    enum ractor_basket_type type;
    VALUE sender;
    st_data_t port_id;

    struct {
        VALUE v;
        bool exception;
        /* RLGCv2 (design_v2.md §4.2): true when v is a Marshal byte
         * String snapshot (the graph contained a type the native copier
         * does not support); the receiver materializes it with
         * Marshal.load instead of the native traversal. */
        bool marshaled;
        /* RLGCv2 (design_v2.md §4.5): for basket_type_move, the off-heap
         * (xmalloc'd) move courier; v is unused for move baskets. */
        struct rb_ractor_move_courier *move_courier;
        /* RLGCv2: native copy snapshot（basket_type_copy かつ !marshaled）の generic-ivar
         * 対応表 {snapshot host -> fields_obj}。送信時に構築（copy_enter）、受信側 materialize
         * が引く（sender の per-Ractor 表を跨がないため）。値 fields_obj は snapshot と一緒に
         * sender の objspace で pin されて生き続けるので、この表を別途 mark する必要はない。
         * 対応表が無い（generic ivar 無し / marshaled / move）ときは NULL。 */
        struct st_table *gen_fields;
    } p; // payload

    struct ccan_list_node node;
};

#if 0
static inline bool
ractor_basket_type_p(const struct ractor_basket *b, enum ractor_basket_type type)
{
    return b->type == type;
}

static inline bool
ractor_basket_none_p(const struct ractor_basket *b)
{
    return ractor_basket_type_p(b, basket_type_none);
}
#endif

static void
ractor_basket_mark(const struct ractor_basket *b)
{
    if (b->type == basket_type_move) {
        /* the courier is off-heap; mark only the shareable VALUEs it carries */
        ractor_move_courier_mark(b->p.move_courier);
    }
    else {
        rb_gc_mark(b->p.v);
    }
}

static void
ractor_basket_free(struct ractor_basket *b)
{
    if (b->type == basket_type_move && b->p.move_courier) {
        /* an unconsumed move courier (e.g. the queue is being torn down) */
        ractor_move_courier_free(b->p.move_courier);
        b->p.move_courier = NULL;
    }
    else if (b->type != basket_type_move && b->p.gen_fields) {
        /* RLGCv2: native copy の generic-ivar 対応表（st は raw malloc）。 */
        st_free_table(b->p.gen_fields);
        b->p.gen_fields = NULL;
    }
    SIZED_FREE(b);
}

static struct ractor_basket *
ractor_basket_alloc(void)
{
    struct ractor_basket *b = ALLOC(struct ractor_basket);
    return b;
}

// ractor-internal - ractor_queue

struct ractor_queue {
    struct ccan_list_head set;
    bool closed;
};

static void
ractor_queue_init(struct ractor_queue *rq)
{
    ccan_list_head_init(&rq->set);
    rq->closed = false;
}

static struct ractor_queue *
ractor_queue_new(void)
{
    struct ractor_queue *rq = ALLOC(struct ractor_queue);
    ractor_queue_init(rq);
    return rq;
}

static void
ractor_queue_mark(const struct ractor_queue *rq)
{
    const struct ractor_basket *b;

    ccan_list_for_each(&rq->set, b, node) {
        ractor_basket_mark(b);
    }
}

static void
ractor_queue_free(struct ractor_queue *rq)
{
    struct ractor_basket *b, *nxt;

    ccan_list_for_each_safe(&rq->set, b, nxt, node) {
        ccan_list_del_init(&b->node);
        ractor_basket_free(b);
    }

    VM_ASSERT(ccan_list_empty(&rq->set));

    SIZED_FREE(rq);
}

RBIMPL_ATTR_MAYBE_UNUSED()
static size_t
ractor_queue_size(const struct ractor_queue *rq)
{
    size_t size = 0;
    const struct ractor_basket *b;

    ccan_list_for_each(&rq->set, b, node) {
        size++;
    }
    return size;
}

static void
ractor_queue_close(struct ractor_queue *rq)
{
    rq->closed = true;
}

static void
ractor_queue_move(struct ractor_queue *dst_rq, struct ractor_queue *src_rq)
{
    struct ccan_list_head *src = &src_rq->set;
    struct ccan_list_head *dst = &dst_rq->set;

    dst->n.next = src->n.next;
    dst->n.prev = src->n.prev;
    dst->n.next->prev = &dst->n;
    dst->n.prev->next = &dst->n;
    ccan_list_head_init(src);
}

#if 0
static struct ractor_basket *
ractor_queue_head(rb_ractor_t *r, struct ractor_queue *rq)
{
    return ccan_list_top(&rq->set, struct ractor_basket, node);
}
#endif

static bool
ractor_queue_empty_p(rb_ractor_t *r, const struct ractor_queue *rq)
{
    return ccan_list_empty(&rq->set);
}

static struct ractor_basket *
ractor_queue_deq(rb_ractor_t *r, struct ractor_queue *rq)
{
    VM_ASSERT(GET_RACTOR() == r);

    return ccan_list_pop(&rq->set, struct ractor_basket, node);
}

static void
ractor_queue_enq(rb_ractor_t *r, struct ractor_queue *rq, struct ractor_basket *basket)
{
    ccan_list_add_tail(&rq->set, &basket->node);
}

#if 0
static void
rq_dump(const struct ractor_queue *rq)
{
    int i=0;
    struct ractor_basket *b;
    ccan_list_for_each(&rq->set, b, node) {
        fprintf(stderr, "%d type:%s %p\n", i, basket_type_name(b->type), (void *)b);
        i++;
    }
}
#endif

static void ractor_delete_port(rb_ractor_t *cr, st_data_t id, bool locked);

static struct ractor_queue *
ractor_get_queue(rb_ractor_t *cr, st_data_t id, bool locked)
{
    VM_ASSERT(cr == GET_RACTOR());

    struct ractor_queue *rq;

    if (cr->sync.ports && st_lookup(cr->sync.ports, id, (st_data_t *)&rq)) {
        if (rq->closed && ractor_queue_empty_p(cr, rq)) {
            ractor_delete_port(cr, id, locked);
            return NULL;
        }
        else {
            return rq;
        }
    }
    else {
        return NULL;
    }
}

// ractor-internal - ports

static void
ractor_add_port(rb_ractor_t *r, st_data_t id)
{
    struct ractor_queue *rq = ractor_queue_new();
    ASSERT_ractor_unlocking(r);

    RUBY_DEBUG_LOG("id:%u", (unsigned int)id);

    RACTOR_LOCK(r);
    {
        st_insert(r->sync.ports, id, (st_data_t)rq);
    }
    RACTOR_UNLOCK(r);
}

static void
ractor_delete_port_locked(rb_ractor_t *cr, st_data_t id)
{
    ASSERT_ractor_locking(cr);

    RUBY_DEBUG_LOG("id:%u", (unsigned int)id);

    struct ractor_queue *rq;

    if (st_delete(cr->sync.ports, &id, (st_data_t *)&rq)) {
        ractor_queue_free(rq);
    }
    else {
        VM_ASSERT(0);
    }
}

static void
ractor_delete_port(rb_ractor_t *cr, st_data_t id, bool locked)
{
    if (locked) {
        ractor_delete_port_locked(cr, id);
    }
    else {
        RACTOR_LOCK_SELF(cr);
        {
            ractor_delete_port_locked(cr, id);
        }
        RACTOR_UNLOCK_SELF(cr);
    }
}

static const struct ractor_port *
ractor_default_port(rb_ractor_t *r)
{
    return RACTOR_PORT_PTR(r->sync.default_port_value);
}

static VALUE
ractor_default_port_value(rb_ractor_t *r)
{
    return r->sync.default_port_value;
}

static bool
ractor_closed_port_p(rb_execution_context_t *ec, rb_ractor_t *r, const struct ractor_port *rp)
{
    VM_ASSERT(rb_ec_ractor_ptr(ec) == rp->r ? 1 : (ASSERT_ractor_locking(rp->r), 1));

    const struct ractor_queue *rq;

    if (rp->r->sync.ports && st_lookup(rp->r->sync.ports, ractor_port_id(rp), (st_data_t *)&rq)) {
        return rq->closed;
    }
    else {
        return true;
    }
}

static void ractor_deliver_incoming_messages(rb_execution_context_t *ec, rb_ractor_t *cr);
static bool ractor_queue_empty_p(rb_ractor_t *r, const struct ractor_queue *rq);

static bool
ractor_close_port(rb_execution_context_t *ec, rb_ractor_t *cr, const struct ractor_port *rp)
{
    VM_ASSERT(cr == rp->r);
    struct ractor_queue *rq = NULL;

    RACTOR_LOCK_SELF(cr);
    {
        ractor_deliver_incoming_messages(ec, cr); // check incoming messages

        if (st_lookup(rp->r->sync.ports, ractor_port_id(rp), (st_data_t *)&rq)) {
            ractor_queue_close(rq);

            if (ractor_queue_empty_p(cr, rq)) {
                // delete from the table
                ractor_delete_port(cr, ractor_port_id(rp), true);
            }

            // TODO: free rq
        }
    }
    RACTOR_UNLOCK_SELF(cr);

    return rq != NULL;
}

static int
ractor_free_all_ports_i(st_data_t port_id, st_data_t val, st_data_t dat)
{
    struct ractor_queue *rq = (struct ractor_queue *)val;
    // rb_ractor_t *cr = (rb_ractor_t *)dat;

    ractor_queue_free(rq);
    return ST_CONTINUE;
}

static void
ractor_free_all_ports(rb_ractor_t *cr)
{
    if (cr->sync.ports) {
        st_foreach(cr->sync.ports, ractor_free_all_ports_i, (st_data_t)cr);
        st_free_table(cr->sync.ports);
        cr->sync.ports = NULL;
    }

    if (cr->sync.recv_queue) {
        ractor_queue_free(cr->sync.recv_queue);
        cr->sync.recv_queue = NULL;
    }
}

#if defined(HAVE_WORKING_FORK)
static void
ractor_sync_terminate_atfork(rb_vm_t *vm, rb_ractor_t *r)
{
    ractor_free_all_ports(r);
    r->sync.legacy = Qnil;
}
#endif

// Ractor#monitor

struct ractor_monitor {
    struct ractor_port port;
    struct ccan_list_node node;
};

/* No GC mark walks r->sync.monitors. The entries only carry a copied
 * port (a ractor pointer + ids, no VALUEs), and the watcher Ractor's
 * object -- the only thing the old walk marked -- is rooted from the
 * VM's ractor set for as long as the watcher lives. Walking here was
 * also unsound: FOREIGN Ractors register/unregister themselves in this
 * list (under r's sync lock), so the owner's lock-free local GC raced
 * their ccan-list pointer updates (TSan: ractor_mark_monitors vs
 * ractor_monitor). */

static VALUE
ractor_exit_token(bool exc)
{
    if (exc) {
        RUBY_DEBUG_LOG("aborted");
        return ID2SYM(idAborted);
    }
    else {
        RUBY_DEBUG_LOG("exited");
        return ID2SYM(idExited);
    }
}

static VALUE
ractor_monitor(rb_execution_context_t *ec, VALUE self, VALUE port)
{
    rb_ractor_t *r = RACTOR_PTR(self);
    bool terminated = false;
    const struct ractor_port *rp = RACTOR_PORT_PTR(port);
    struct ractor_monitor *rm = ALLOC(struct ractor_monitor);
    rm->port = *rp; // copy port information

    RACTOR_LOCK(r);
    {
        if (UNDEF_P(r->sync.legacy)) { // not terminated
            RUBY_DEBUG_LOG("OK/r:%u -> port:%u@r%u", (unsigned int)rb_ractor_id(r), (unsigned int)ractor_port_id(&rm->port), (unsigned int)rb_ractor_id(rm->port.r));
            ccan_list_add_tail(&r->sync.monitors, &rm->node);
        }
        else {
            RUBY_DEBUG_LOG("NG/r:%u -> port:%u@r%u", (unsigned int)rb_ractor_id(r), (unsigned int)ractor_port_id(&rm->port), (unsigned int)rb_ractor_id(rm->port.r));
            terminated = true;
        }
    }
    RACTOR_UNLOCK(r);

    if (terminated) {
        SIZED_FREE(rm);
        ractor_port_send(ec, port, ractor_exit_token(r->sync.legacy_exc), Qfalse);

        return Qfalse;
    }
    else {
        return Qtrue;
    }
}

static VALUE
ractor_unmonitor(rb_execution_context_t *ec, VALUE self, VALUE port)
{
    rb_ractor_t *r = RACTOR_PTR(self);
    const struct ractor_port *rp = RACTOR_PORT_PTR(port);

    RACTOR_LOCK(r);
    {
        if (UNDEF_P(r->sync.legacy)) { // not terminated
            struct ractor_monitor *rm, *nxt;

            ccan_list_for_each_safe(&r->sync.monitors, rm, nxt, node) {
                if (ractor_port_id(&rm->port) == ractor_port_id(rp)) {
                    RUBY_DEBUG_LOG("r:%u -> port:%u@r%u",
                                   (unsigned int)rb_ractor_id(r),
                                   (unsigned int)ractor_port_id(&rm->port),
                                   (unsigned int)rb_ractor_id(rm->port.r));
                    ccan_list_del(&rm->node);
                    SIZED_FREE(rm);
                }
            }
        }
    }
    RACTOR_UNLOCK(r);

    return self;
}

static void
ractor_notify_exit(rb_execution_context_t *ec, rb_ractor_t *cr, VALUE legacy, bool exc)
{
    RUBY_DEBUG_LOG("exc:%d", exc);
    VM_ASSERT(!UNDEF_P(legacy));
    VM_ASSERT(cr->sync.legacy == Qundef);

    RACTOR_LOCK_SELF(cr);
    {
        ractor_free_all_ports(cr);

        cr->sync.legacy = legacy;
        cr->sync.legacy_exc = exc;
    }
    RACTOR_UNLOCK_SELF(cr);

    // send token

    VALUE token = ractor_exit_token(exc);
    struct ractor_monitor *rm, *nxt;

    ccan_list_for_each_safe(&cr->sync.monitors, rm, nxt, node)
    {
        RUBY_DEBUG_LOG("port:%u@r%u", (unsigned int)ractor_port_id(&rm->port), (unsigned int)rb_ractor_id(rm->port.r));

        ractor_try_send(ec, &rm->port, token, false);

        ccan_list_del(&rm->node);
        SIZED_FREE(rm);
    }

    VM_ASSERT(ccan_list_empty(&cr->sync.monitors));
}

// ractor-internal - initialize, mark, free, memsize

static int
ractor_mark_ports_i(st_data_t key, st_data_t val, st_data_t data)
{
    // id -> ractor_queue
    const struct ractor_queue *rq = (struct ractor_queue *)val;
    ractor_queue_mark(rq);
    return ST_CONTINUE;
}

static void
ractor_sync_mark(rb_ractor_t *r)
{
    /* default_port_value is a stable single slot (set once at creation,
     * written by the owner as one aligned word): safe to read from any GC. */
    rb_gc_mark(r->sync.default_port_value);

    /* RLGCv2 M1b: the queues, the port table, the monitor list AND the
     * materialize-frame chain are mutated by the owner under its sync
     * lock (or, for the frame chain, written by the owner during a
     * receive), so a lock-free foreign mark (main's local GC traversing this
     * Ractor object) reads them torn -- and by containment everything in
     * them is foreign to that marker anyway (payload snapshots stay alive
     * through the sender's in-flight pin: the shref for copy, the move
     * manager for move; ports through the shareable pin). Walk them only
     * when no concurrent owner can exist: our own Ractor, a terminated one,
     * or under the global GC's barrier. */
    rb_ractor_t *cr = rb_current_ractor_raw(false);
    if (r == cr || rb_ractor_status_p(r, ractor_terminated) || rb_gc_during_global_gc_p()) {
        /* snapshots/couriers being materialized by receives (baskets
         * already popped); off the queue, rooted only here for the global
         * GC's re-pin. A chain: nested receives from user load hooks each
         * push a frame. A foreign marker must not read it. */
        for (const struct rlgc_materialize_frame *f = r->sync.materialize_frames;
             f != NULL; f = f->prev) {
            rb_gc_mark(f->snapshot);
            /* the move courier is off-heap; mark the shareable VALUEs it
             * carries so a concurrent global GC keeps them */
            ractor_move_courier_mark(f->courier);
        }
        if (r->sync.ports) {
            /* The recv_queue (and the ports table) are written by foreign
             * SENDERS that hold r's sync lock (ractor_queue_enq under
             * RACTOR_LOCK). When this is our own concurrent local GC
             * (r == cr, and not a stop-the-world global GC) a sender on
             * another thread can mutate the queue while we walk it -- a real
             * data race (ractor_queue_mark vs ractor_queue_enq). Take the lock
             * to exclude senders. This cannot self-deadlock: holding any
             * ractor lock disables malloc-triggered GC (malloc_gc_disabled,
             * gc.c), so a GC marker never itself already holds r's lock. Under
             * a global GC every sender is stopped, and a terminated Ractor has
             * none, so neither of those cases needs the lock. */
            bool lock_against_senders = (r == cr) && !rb_gc_during_global_gc_p();
            if (lock_against_senders) RACTOR_LOCK(r);
            ractor_queue_mark(r->sync.recv_queue);
            st_foreach(r->sync.ports, ractor_mark_ports_i, 0);
            if (lock_against_senders) RACTOR_UNLOCK(r);
        }
        /* monitors are not walked -- see the comment above
         * ractor_monitor's data structures */
    }
}

static void
ractor_queue_repin_in_flight(const struct ractor_queue *rq)
{
    const struct ractor_basket *b;
    ccan_list_for_each(&rq->set, b, node) {
        /* move baskets carry an off-heap courier (no shref to re-pin); their
         * shareable VALUEs are marked through ractor_basket_mark instead. */
        if (b->type == basket_type_copy) {
            rb_gc_pin_in_flight_message(b->p.v);
        }
    }
}

static int
ractor_repin_ports_i(st_data_t key, st_data_t val, st_data_t data)
{
    ractor_queue_repin_in_flight((struct ractor_queue *)val);
    return ST_CONTINUE;
}

/* RLGCv2 (design_v2.md §2.2 step 6): the global GC clears all shref bits,
 * so every in-flight payload (queued baskets and the snapshot a receive
 * is currently materializing) must be re-pinned before the unified mark.
 * Runs on the driver under the barrier. */
void
rb_ractor_repin_in_flight(rb_ractor_t *r)
{
    if (r->sync.ports) {
        ractor_queue_repin_in_flight(r->sync.recv_queue);
        st_foreach(r->sync.ports, ractor_repin_ports_i, 0);
    }
    for (const struct rlgc_materialize_frame *f = r->sync.materialize_frames;
         f != NULL; f = f->prev) {
        if (f->snapshot && !RB_SPECIAL_CONST_P(f->snapshot)) {
            rb_gc_pin_in_flight_message(f->snapshot);
        }
    }
}

static int
ractor_sync_free_ports_i(st_data_t _key, st_data_t val, st_data_t _args)
{
    struct ractor_queue *queue = (struct ractor_queue *)val;

    ractor_queue_free(queue);

    return ST_CONTINUE;
}

static void
ractor_sync_free(rb_ractor_t *r)
{
    if (r->sync.recv_queue) {
        ractor_queue_free(r->sync.recv_queue);
    }

    // maybe NULL
    if (r->sync.ports) {
        st_foreach(r->sync.ports, ractor_sync_free_ports_i, 0);
        st_free_table(r->sync.ports);
        r->sync.ports = NULL;
    }
}

static size_t
ractor_sync_memsize(const rb_ractor_t *r)
{
    if (r->sync.ports) {
        return st_table_size(r->sync.ports);
    }
    else {
        return 0;
    }
}

static void
ractor_sync_init(rb_ractor_t *r)
{
    // lock
    rb_native_mutex_initialize(&r->sync.lock);

    // monitors
    ccan_list_head_init(&r->sync.monitors);

    // waiters
    ccan_list_head_init(&r->sync.waiters);

    // receiving queue
    r->sync.recv_queue = ractor_queue_new();

    // ports
    r->sync.ports = st_init_numtable();
    r->sync.default_port_value = ractor_port_new(r);
    FL_SET_RAW(r->sync.default_port_value, RUBY_FL_SHAREABLE); // only default ports are shareable
    rb_gc_obj_became_shareable(r->sync.default_port_value);

    // legacy
    r->sync.legacy = Qundef;

    // RLGCv2: no receive is rebuilding a payload yet
    r->sync.materialize_frames = NULL;

#ifndef RUBY_THREAD_PTHREAD_H
    rb_native_cond_initialize(&r->sync.wakeup_cond);
#endif
}

// Ractor#value

static rb_ractor_t *
ractor_set_successor_once(rb_ractor_t *r, rb_ractor_t *cr)
{
    if (r->sync.successor == NULL) {
        rb_ractor_t *successor = ATOMIC_PTR_CAS(r->sync.successor, NULL, cr);
        return successor == NULL ? cr : successor;
    }

    return r->sync.successor;
}

static VALUE ractor_reset_belonging(VALUE obj);

static VALUE
ractor_make_remote_exception(VALUE cause, VALUE sender)
{
    VALUE err = rb_exc_new_cstr(rb_eRactorRemoteError, "thrown by remote Ractor.");
    rb_ivar_set(err, rb_intern("@ractor"), sender);
    rb_ec_setup_exception(NULL, err, cause);
    return err;
}

/* RLGCv2 (design_v2.md section 4.3): after Ractor#value absorbed the
 * dead Ractor's objspace, everything still referenced from its C struct
 * (the legacy value for repeat #value calls, its stdio, its local
 * storage) belongs to the CALLER's objspace but is reachable only
 * through the Ractor object, which usually lives in some other
 * Ractor's objspace -- whose marks foreign-skip our objects, while our
 * own GC never traverses the foreign Ractor object. Pin each top-level
 * slot with the shref bit: our pages, our thread, plain stores. Their
 * children survive through the normal root traversal, and the global
 * GC re-derives these exact bits from the shareable Ractor object's
 * s->u edges for as long as the Ractor object lives. */
void
rb_ractor_pin_inherited_parts(rb_ractor_t *r)
{
    VALUE slots[] = {
        r->sync.legacy,
        r->r_stdin, r->r_stdout, r->r_stderr,
        r->verbose, r->debug,
    };
    for (size_t i = 0; i < numberof(slots); i++) {
        if (!SPECIAL_CONST_P(slots[i])) {
            rb_gc_pin_in_flight_message(slots[i]);
        }
    }

    /* The dead Ractor's local storage is unreachable to Ruby code from
     * now on (Ractor#[] works only from inside): release it here rather
     * than pinning it -- its values can then die naturally, and neither
     * ractor_mark nor ractor_free walks a stale table later. */
    ractor_local_storage_free(r);
    r->local_storage = NULL;
    r->idkey_local_storage = NULL;

    /* The dead Ractor's main thread stays on its threads list, and its
     * Thread/Fiber wrapper objects were born in the dead objspace
     * (thread.c, rb_thread_create_ractor) -- inherited with everything
     * else. Pinning the wrappers is enough: their dmarks reach the rest
     * of the thread state (th->value and friends) transitively. */
    rb_thread_t *th = 0;
    ccan_list_for_each(&r->threads.set, th, lt_node) {
        if (th->self && !SPECIAL_CONST_P(th->self)) {
            rb_gc_pin_in_flight_message(th->self);
        }
        if (th->root_fiber) {
            VALUE fself = rb_fiberptr_self(th->root_fiber);
            if (fself && !SPECIAL_CONST_P(fself)) {
                rb_gc_pin_in_flight_message(fself);
            }
        }
        if (th->ec && th->ec->fiber_ptr) {
            VALUE fself = rb_fiberptr_self(th->ec->fiber_ptr);
            if (fself && !SPECIAL_CONST_P(fself)) {
                rb_gc_pin_in_flight_message(fself);
            }
        }
    }
}

static VALUE
ractor_value(rb_execution_context_t *ec, VALUE self)
{
    rb_ractor_t *cr = rb_ec_ractor_ptr(ec);
    rb_ractor_t *r = RACTOR_PTR(self);
    rb_ractor_t *sr = ractor_set_successor_once(r, cr);

    if (sr == cr) {
        /* RLGCv2 (design_v2.md section 4.3): the value is returned by
         * reference, so inherit the dead Ractor's objspace into ours
         * first -- after the merge the return value is our own object
         * and containment holds without any copy.
         * The monitor-port wakeup precedes the end of the dying thread's
         * teardown (vm_remove_ractor still touches the objspace), so wait
         * for the terminated status, which is set under the VM lock after
         * the teardown's last objspace access. */
        while (!rb_ractor_status_p(r, ractor_terminated)) {
            rb_thread_schedule();
        }

        /* RLGCv2: r の per-Ractor generic_fields 表を joiner へ移送する。これは objspace
         * merge より「前」に行う必要がある: rb_gc_objspace_absorb_into_current は内部で
         * src(=r) の objspace を gc_sweep_rest で掃くので、その最中に r の dead host が
         * obj_free→rb_free_generic_ivar を呼ぶ。その時点の GET_RACTOR() は joiner なので
         * entry を joiner 表に引きに行く。先に移送しておかないと「objspace は移ったが
         * 登録情報が未移送」の窓（freeze-hash と同型）で miss する。移送〜merge 間に GC
         * safepoint は無く、移送先 entry の key はまだ r の objspace に居るが merge 前に
         * 誰も引かないので安全。 */
        rb_ractor_absorb_generic_fields(GET_RACTOR(), r);

        rb_gc_objspace_absorb_into_current(&r->objspace);

        /* RLGCv2: join した Ractor r の登録済み VM グローバル root を joiner へ
         * 移管し、joiner の local GC がその不滅オブジェクトを生かし続けるようにする。 */
        rb_ractor_absorb_registered_globals(GET_RACTOR(), r);

        /* inherit したオブジェクトは今や我々のものだが、それらへの唯一の
         * 経路は死んだ Ractor の C struct であり、Ractor オブジェクトを
         * 所有する者（通常は別の Ractor で、その mark は我々のオブジェクトを
         * foreign-skip する）だけがそれを traverse する。それらを shref
         * ビットで pin する（今や我々がそのページを所有するので通常のストア）。
         * すると我々の local GC がそれらを root し、次の global GC は Ractor
         * オブジェクト自体が生存する間ずっと、shareable-Ractor-object ->
         * unshareable のエッジから同一のビットを再導出する。それはまさに
         * それらの生存期間に等しい。 */
        rb_ractor_pin_inherited_parts(r);

        ractor_reset_belonging(r->sync.legacy);

        if (r->sync.legacy_exc) {
            rb_exc_raise(ractor_make_remote_exception(r->sync.legacy, self));
        }
        return r->sync.legacy;
    }
    else {
        rb_raise(rb_eRactorError, "Only the successor ractor can take a value");
    }
}

static VALUE ractor_copy_native_try(VALUE obj); // in ractor.c

static VALUE
ractor_marshal_dump_body(VALUE obj)
{
    return rb_marshal_dump(obj, Qnil);
}

static VALUE
ractor_marshal_dump_rescue(VALUE obj, VALUE errinfo)
{
    rb_raise(rb_eRactorError, "can not copy %"PRIsVALUE" object.", rb_class_of(obj));
    UNREACHABLE_RETURN(Qnil);
}

static VALUE
ractor_prepare_payload(rb_execution_context_t *ec, VALUE obj, enum ractor_basket_type *ptype, bool *pmarshaled)
{
    switch (*ptype) {
      case basket_type_ref:
        return obj;
      default:
        if (rb_ractor_shareable_p(obj)) {
            *ptype = basket_type_ref;
            return obj;
        }
        else {
            /* design_v2.md §4.2 / decision 11: snapshot copy on the
             * sender, without calling the user-visible #clone. Core
             * types are deep-copied natively; any other type makes the
             * snapshot a Marshal byte string (whose user hooks run here,
             * on the sender, like #clone hooks used to). */
            *ptype = basket_type_copy;
            /* RLGCv2: native copy 中、copy_enter が snapshot の generic-ivar host の
             * fields_obj を cr->gen_fields_capture に記録する（host が出て初めて遅延確保）。
             * ractor_basket_new が basket に移して回収する。Marshal fallback 時は破棄。 */
            rb_ractor_t *cr = rb_ec_ractor_ptr(ec);
            VM_ASSERT(!cr->gen_fields_capturing && cr->gen_fields_capture == NULL);
            cr->gen_fields_capturing = true;
            VALUE snapshot = Qundef;
            /* the native copy can raise (allocation, async interrupt);
             * a stuck capturing flag would fail the next send's assert
             * and leak a stale capture map into its basket */
            enum ruby_tag_type state;
            EC_PUSH_TAG(ec);
            if ((state = EC_EXEC_TAG()) == TAG_NONE) {
                snapshot = ractor_copy_native_try(obj);
            }
            EC_POP_TAG();
            cr->gen_fields_capturing = false;
            if (state != TAG_NONE) {
                if (cr->gen_fields_capture) {
                    st_free_table(cr->gen_fields_capture);
                    cr->gen_fields_capture = NULL;
                }
                EC_JUMP_TAG(ec, state);
            }
            if (UNDEF_P(snapshot)) {
                if (cr->gen_fields_capture) {
                    st_free_table(cr->gen_fields_capture);
                    cr->gen_fields_capture = NULL;
                }
                snapshot = rb_rescue2(ractor_marshal_dump_body, obj,
                                      ractor_marshal_dump_rescue, obj,
                                      rb_eTypeError, (VALUE)0);
                *pmarshaled = true;
            }
            return snapshot;
        }
    }
}

static struct ractor_basket *
ractor_basket_new(rb_execution_context_t *ec, VALUE obj, enum ractor_basket_type type, bool exc)
{
    struct ractor_basket *b = ractor_basket_alloc();
    b->p.exception = exc;
    b->p.marshaled = false;
    b->p.move_courier = NULL;
    b->p.gen_fields = NULL;

    if (type == basket_type_move) {
        /* RLGCv2 (design_v2.md §4.5): serialize the graph into an off-heap
         * courier; the originals become RactorMovedObject. Nothing in flight
         * is a GC object, so the sender's GC never marks/sweeps/moves it. */
        b->type = basket_type_move;
        b->p.v = Qfalse;
        b->p.move_courier = ractor_move_courier_build(obj);
    }
    else {
        bool marshaled = false;
        VALUE v = ractor_prepare_payload(ec, obj, &type, &marshaled);
        if (type == basket_type_copy) {
            /* RLGCv2: the copy snapshot (native graph or Marshal string)
             * lives in the sender's objspace until the receiver materializes
             * it.  Pin it (shref) so the sender's confined GC keeps it. */
            rb_gc_pin_in_flight_message(v);
            /* RLGCv2: native copy の generic-ivar 対応表を basket へ移す（prepare_payload
             * が cr->gen_fields_capture に構築、marshaled/generic-ivar 無しなら空/NULL）。 */
            b->p.gen_fields = rb_ec_ractor_ptr(ec)->gen_fields_capture;
            rb_ec_ractor_ptr(ec)->gen_fields_capture = NULL;
        }
        b->type = type;
        b->p.v = v;
        b->p.marshaled = marshaled;
    }
    return b;
}

/* RLGCv2: true while this Ractor is materializing an incoming copy
 * (ractor_basket_value -> ractor_copy_native_try). During that window the
 * half-built result legitimately holds edges into the sender-resident snapshot
 * (pinned via sync.in_flight_materializing), so the confined-GC verifier must
 * not flag those as containment violations -- the copy's own allocations can
 * trigger that GC mid-traversal. */
bool
rb_gc_current_ractor_materializing_p(void)
{
    const rb_ractor_t *cr = rb_current_ractor_raw(false);
    if (cr == NULL) return false;
    /* true only for a COPY materialize (snapshot != Qfalse): move shells
     * reference other shells in this objspace, never the sender's graph */
    for (const struct rlgc_materialize_frame *f = cr->sync.materialize_frames;
         f != NULL; f = f->prev) {
        if (f->snapshot != Qfalse) return true;
    }
    return false;
}

static VALUE
ractor_basket_value(struct ractor_basket *b)
{
    switch (b->type) {
      case basket_type_ref:
        break;
      case basket_type_copy: {
        /* RLGCv2 M3 (design_v2.md §4.2): materialize the sender-side
         * snapshot into the receiving Ractor's objspace. Handing the
         * sender-resident graph over by reference would create
         * unshareable cross-objspace edges that neither confined GC may
         * traverse (the receiver's stores into it would also bypass the
         * owner's write barrier accounting). The snapshot stays pinned
         * (in-flight shref) in the sender's objspace and becomes garbage
         * there once this copy is made. Marshal.load allocates through
         * the ordinary newobj/write-barrier paths of this Ractor.
         * The basket is already off the queue, so the materialize frame
         * is what keeps the snapshot rooted (and re-pinnable by a global
         * GC) for the duration of the copy.
         *
         * The rebuild can raise -- marshal_load/_load hooks and autoload
         * are user code, and async interrupts (Timeout, Thread#raise)
         * can land anywhere in it -- and those same hooks can run a
         * nested Ractor.receive. Push a machine-stack frame (nesting)
         * and pop it under TAG protection (unwind), so the chain never
         * leaks a dead materialization or drops an outer one. */
        rb_execution_context_t *ec = rb_current_ec_noinline();
        rb_ractor_t *cr = rb_ec_ractor_ptr(ec);
        struct rlgc_materialize_frame frame = {
            .snapshot = b->p.v, .courier = NULL, .prev = cr->sync.materialize_frames,
        };
        cr->sync.materialize_frames = &frame;
        struct st_table *prev_gf = cr->gen_fields_materialize;
        VALUE result = Qundef;
        enum ruby_tag_type state;
        EC_PUSH_TAG(ec);
        if ((state = EC_EXEC_TAG()) == TAG_NONE) {
            if (b->p.marshaled) {
                result = rb_marshal_load(b->p.v);
            }
            else {
                /* RLGCv2: materialize 中、snapshot host の generic-ivar を読むとき（native copy
                 * の rb_copy_generic_ivar）、sender の per-Ractor 表を跨がずこの対応表から
                 * fields_obj を引く（rb_obj_fields_generic_uncached が gen_fields_materialize
                 * を参照）。 */
                cr->gen_fields_materialize = b->p.gen_fields;
                result = ractor_copy_native_try(b->p.v);
                if (UNDEF_P(result)) rb_bug("ractor_basket_value: native snapshot not natively copyable");
            }
        }
        EC_POP_TAG();
        cr->gen_fields_materialize = prev_gf;
        cr->sync.materialize_frames = frame.prev;
        if (state != TAG_NONE) EC_JUMP_TAG(ec, state);
        /* keep the result stack-rooted past the frame being popped */
        ractor_reset_belonging(result);
        b->p.v = result;
        RB_GC_GUARD(result);
        break;
      }
      case basket_type_move: {
        /* RLGCv2 (design_v2.md §4.5): rebuild the moved graph from the
         * off-heap courier into THIS Ractor's objspace. The originals are
         * already RactorMovedObject (set when the courier was built), so
         * move's snapshot semantics hold. The courier is xmalloc'd, not a GC
         * object, so the sender's concurrent confined GC never marked, swept,
         * moved or raced it -- no keep-alive trick, no STW. The only VALUEs it
         * carries are shareables/immediates; the materialize frame roots them
         * for a global GC while we rebuild.
         *
         * The rebuild can raise here too (rb_hash_aset on moved keys with a
         * custom #hash runs user code; async interrupts): same frame + TAG
         * discipline. On a raise the courier stays owned by the basket
         * (b->p.move_courier != NULL), so basket teardown frees it. */
        rb_execution_context_t *ec = rb_current_ec_noinline();
        rb_ractor_t *cr = rb_ec_ractor_ptr(ec);
        struct rb_ractor_move_courier *courier = b->p.move_courier;
        struct rlgc_materialize_frame frame = {
            .snapshot = Qfalse, .courier = courier, .prev = cr->sync.materialize_frames,
        };
        cr->sync.materialize_frames = &frame;
        /* Keep the materialized graph on the machine stack (result) across the
         * whole post-materialize sequence. Once the frame is popped it
         * is the ONLY root for the graph until it reaches the caller's stack;
         * ractor_move_courier_free walks a big free-loop here, a wide enough
         * window for a concurrent global GC (main's GC.start(full)) to collect
         * the graph if it lived only in the malloc'd basket's p.v. */
        VALUE result = Qundef;
        enum ruby_tag_type state;
        EC_PUSH_TAG(ec);
        if ((state = EC_EXEC_TAG()) == TAG_NONE) {
            result = ractor_move_courier_materialize(courier);
        }
        EC_POP_TAG();
        cr->sync.materialize_frames = frame.prev;
        if (state != TAG_NONE) EC_JUMP_TAG(ec, state);
        ractor_move_courier_free(courier);
        b->p.move_courier = NULL;
        ractor_reset_belonging(result);
        b->p.v = result;
        RB_GC_GUARD(result);
        break;
      }
      default:
        VM_ASSERT(0); // unreachable
    }

    VM_ASSERT(!RB_TYPE_P(b->p.v, T_NONE));
    return b->p.v;
}

static VALUE
ractor_basket_accept(struct ractor_basket *b)
{
    VALUE v = ractor_basket_value(b);

    if (b->p.exception) {
        VALUE err = ractor_make_remote_exception(v, b->sender);
        ractor_basket_free(b);
        rb_exc_raise(err);
    }

    ractor_basket_free(b);
    return v;
}

// Ractor blocking by receive

#if VM_CHECK_MODE > 0
static bool
ractor_waiter_included(rb_ractor_t *cr, rb_thread_t *th)
{
    ASSERT_ractor_locking(cr);

    struct ractor_waiter *w;

    ccan_list_for_each(&cr->sync.waiters, w, node) {
        if (w->th == th) {
            return true;
        }
    }

    return false;
}
#endif

#if USE_RUBY_DEBUG_LOG

static const char *
wakeup_status_str(enum ractor_wakeup_status wakeup_status)
{
    switch (wakeup_status) {
      case wakeup_none: return "none";
      case wakeup_by_send: return "by_send";
      case wakeup_by_interrupt: return "by_interrupt";
      // case wakeup_by_close: return "by_close";
    }
    rb_bug("unreachable");
}

static const char *
basket_type_name(enum ractor_basket_type type)
{
    switch (type) {
      case basket_type_none: return  "none";
      case basket_type_ref: return "ref";
      case basket_type_copy: return "copy";
      case basket_type_move: return "move";
    }
    VM_ASSERT(0);
    return NULL;
}

#endif // USE_RUBY_DEBUG_LOG

#ifdef RUBY_THREAD_PTHREAD_H

//

#else // win32

static void
ractor_cond_wait(rb_ractor_t *r)
{
#if RACTOR_CHECK_MODE > 0
    VALUE locked_by = r->sync.locked_by;
    r->sync.locked_by = Qnil;
#endif
    rb_native_cond_wait(&r->sync.wakeup_cond, &r->sync.lock);

#if RACTOR_CHECK_MODE > 0
    r->sync.locked_by = locked_by;
#endif
}

static void *
ractor_wait_no_gvl(void *ptr)
{
    struct ractor_waiter *waiter = (struct ractor_waiter *)ptr;
    rb_ractor_t *cr = waiter->th->ractor;

    RACTOR_LOCK_SELF(cr);
    {
        if (waiter->wakeup_status == wakeup_none) {
            ractor_cond_wait(cr);
        }
    }
    RACTOR_UNLOCK_SELF(cr);
    return NULL;
}

static void
rb_ractor_sched_wait(rb_execution_context_t *ec, rb_ractor_t *cr, rb_unblock_function_t *ubf, void *ptr)
{
    struct ractor_waiter *waiter = (struct ractor_waiter *)ptr;

    RACTOR_UNLOCK(cr);
    {
        rb_nogvl(ractor_wait_no_gvl, waiter,
                 ubf, waiter,
                 RB_NOGVL_UBF_ASYNC_SAFE | RB_NOGVL_INTR_FAIL);
    }
    RACTOR_LOCK(cr);
}

static void
rb_ractor_sched_wakeup(rb_ractor_t *r, rb_thread_t *th)
{
    // ractor lock is acquired
    rb_native_cond_broadcast(&r->sync.wakeup_cond);
}
#endif

static bool
ractor_wakeup_all(rb_ractor_t *r, enum ractor_wakeup_status wakeup_status)
{
    ASSERT_ractor_unlocking(r);

    RUBY_DEBUG_LOG("r:%u wakeup:%s", rb_ractor_id(r), wakeup_status_str(wakeup_status));

    bool wakeup_p = false;

    RACTOR_LOCK(r);
    while (1) {
        struct ractor_waiter *waiter = ccan_list_pop(&r->sync.waiters, struct ractor_waiter, node);

        if (waiter) {
            VM_ASSERT(waiter->wakeup_status == wakeup_none);

            waiter->wakeup_status = wakeup_status;
            rb_ractor_sched_wakeup(r, waiter->th);

            wakeup_p = true;
        }
        else {
            break;
        }
    }
    RACTOR_UNLOCK(r);

    return wakeup_p;
}

static void
ubf_ractor_wait(void *ptr)
{
    struct ractor_waiter *waiter = (struct ractor_waiter *)ptr;

    rb_thread_t *th = waiter->th;
    rb_ractor_t *r = th->ractor;
    rb_atomic_t event_serial = waiter->event_serial;

    // clear ubf and nobody can kick UBF
    th->unblock.func = NULL;
    th->unblock.arg  = NULL;

    rb_native_mutex_unlock(&th->interrupt_lock);
    {
        RACTOR_LOCK(r);
        {
            if (RUBY_ATOMIC_LOAD(th->unblock.event_serial) == event_serial && waiter->wakeup_status == wakeup_none) {
                RUBY_DEBUG_LOG("waiter:%p", (void *)waiter);

                waiter->wakeup_status = wakeup_by_interrupt;
                ccan_list_del(&waiter->node);

                rb_ractor_sched_wakeup(r, waiter->th);
            }
        }
        RACTOR_UNLOCK(r);
    }
    rb_native_mutex_lock(&th->interrupt_lock);
}

static enum ractor_wakeup_status
ractor_wait(rb_execution_context_t *ec, rb_ractor_t *cr)
{
    rb_thread_t *th = rb_ec_thread_ptr(ec);

    struct ractor_waiter waiter = {
        .wakeup_status = wakeup_none,
        .th = th,
    };

    RUBY_DEBUG_LOG("wait%s", "");

    ASSERT_ractor_locking(cr);

    VM_ASSERT(GET_RACTOR() == cr);
    VM_ASSERT(!ractor_waiter_included(cr, th));

    ccan_list_add_tail(&cr->sync.waiters, &waiter.node);

    // resume another ready thread and wait for an event
    rb_ractor_sched_wait(ec, cr, ubf_ractor_wait, &waiter);

    if (waiter.wakeup_status == wakeup_none) {
        ccan_list_del(&waiter.node);
    }

    RUBY_DEBUG_LOG("wakeup_status:%s", wakeup_status_str(waiter.wakeup_status));

    RACTOR_UNLOCK_SELF(cr);
    {
        rb_ec_check_ints(ec);
    }
    RACTOR_LOCK_SELF(cr);

    VM_ASSERT(!ractor_waiter_included(cr, th));
    return waiter.wakeup_status;
}

static void
ractor_deliver_incoming_messages(rb_execution_context_t *ec, rb_ractor_t *cr)
{
    ASSERT_ractor_locking(cr);
    struct ractor_queue *recv_q = cr->sync.recv_queue;

    struct ractor_basket *b;
    while ((b = ractor_queue_deq(cr, recv_q)) != NULL) {
        ractor_queue_enq(cr, ractor_get_queue(cr, b->port_id, true), b);
    }
}

static bool
ractor_check_received(rb_ractor_t *cr, struct ractor_queue *messages)
{
    struct ractor_queue *received_queue = cr->sync.recv_queue;
    bool received = false;

    ASSERT_ractor_locking(cr);

    if (ractor_queue_empty_p(cr, received_queue)) {
        RUBY_DEBUG_LOG("empty");
    }
    else {
        received = true;

        // messages <- incoming
        ractor_queue_init(messages);
        ractor_queue_move(messages, received_queue);
    }

    VM_ASSERT(ractor_queue_empty_p(cr, received_queue));

    RUBY_DEBUG_LOG("received:%d", received);
    return received;
}

static void
ractor_wait_receive(rb_execution_context_t *ec, rb_ractor_t *cr)
{
    struct ractor_queue messages;
    bool deliverred = false;

    RACTOR_LOCK_SELF(cr);
    {
        if (ractor_check_received(cr, &messages)) {
            deliverred = true;
        }
        else {
            ractor_wait(ec, cr);
        }
    }
    RACTOR_UNLOCK_SELF(cr);

    if (deliverred) {
        VM_ASSERT(!ractor_queue_empty_p(cr, &messages));
        struct ractor_basket *b;

        while ((b = ractor_queue_deq(cr, &messages)) != NULL) {
            ractor_queue_enq(cr, ractor_get_queue(cr, b->port_id, false), b);
        }
    }
}

static VALUE
ractor_try_receive(rb_execution_context_t *ec, rb_ractor_t *cr, const struct ractor_port *rp)
{
    struct ractor_queue *rq = ractor_get_queue(cr, ractor_port_id(rp), false);

    if (rq == NULL) {
        rb_raise(rb_eRactorClosedError, "The port was already closed");
    }

    struct ractor_basket *b = ractor_queue_deq(cr, rq);

    if (rq->closed && ractor_queue_empty_p(cr, rq)) {
        ractor_delete_port(cr, ractor_port_id(rp), false);
    }

    if (b) {
        return ractor_basket_accept(b);
    }
    else {
        return Qundef;
    }
}

static VALUE
ractor_receive(rb_execution_context_t *ec, const struct ractor_port *rp)
{
    rb_ractor_t *cr = rb_ec_ractor_ptr(ec);
    VM_ASSERT(cr == rp->r);

    RUBY_DEBUG_LOG("port:%u", (unsigned int)ractor_port_id(rp));

    while (1) {
        VALUE v = ractor_try_receive(ec, cr, rp);

        if (v != Qundef) {
            return v;
        }
        else {
            ractor_wait_receive(ec, cr);
        }
    }
}

// Ractor#send

static void
ractor_send_basket(rb_execution_context_t *ec, const struct ractor_port *rp, struct ractor_basket *b, bool raise_on_error)
{
    bool closed = false;

    RUBY_DEBUG_LOG("port:%u@r%u b:%s v:%p", (unsigned int)ractor_port_id(rp), rb_ractor_id(rp->r), basket_type_name(b->type), (void *)b->p.v);

    RACTOR_LOCK(rp->r);
    {
        if (ractor_closed_port_p(ec, rp->r, rp)) {
            closed = true;
        }
        else {
            b->port_id = ractor_port_id(rp);
            ractor_queue_enq(rp->r, rp->r->sync.recv_queue, b);
        }
    }
    RACTOR_UNLOCK(rp->r);

    // NOTE: ref r -> b->p.v is created, but Ractor is unprotected object, so no problem on that.

    if (!closed) {
        ractor_wakeup_all(rp->r, wakeup_by_send);
    }
    else {
        RUBY_DEBUG_LOG("closed:%u@r%u", (unsigned int)ractor_port_id(rp), rb_ractor_id(rp->r));

        if (raise_on_error) {
            ractor_basket_free(b);
            rb_raise(rb_eRactorClosedError, "The port was already closed");
        }
    }
}

static VALUE
ractor_send0(rb_execution_context_t *ec, const struct ractor_port *rp, VALUE obj, VALUE move, bool raise_on_error)
{
    struct ractor_basket *b = ractor_basket_new(ec, obj, RTEST(move) ? basket_type_move : basket_type_none, false);
    ractor_send_basket(ec, rp, b, raise_on_error);
    RB_GC_GUARD(obj);
    return rp->r->pub.self;
}

static VALUE
ractor_send(rb_execution_context_t *ec, const struct ractor_port *rp, VALUE obj, VALUE move)
{
    return ractor_send0(ec, rp, obj, move, true);
}

static VALUE
ractor_try_send(rb_execution_context_t *ec, const struct ractor_port *rp, VALUE obj, VALUE move)
{
    return ractor_send0(ec, rp, obj, move, false);
}

// Ractor::Selector

struct ractor_selector {
    struct st_table *ports; // rpv -> rp

};

static int
ractor_selector_mark_i(st_data_t key, st_data_t val, st_data_t dmy)
{
    rb_gc_mark((VALUE)key); // rpv

    return ST_CONTINUE;
}

static void
ractor_selector_mark(void *ptr)
{
    struct ractor_selector *s = ptr;

    if (s->ports) {
        st_foreach(s->ports, ractor_selector_mark_i, 0);
    }
}

static void
ractor_selector_free(void *ptr)
{
    struct ractor_selector *s = ptr;
    st_free_table(s->ports);
    SIZED_FREE(s);
}

static size_t
ractor_selector_memsize(const void *ptr)
{
    const struct ractor_selector *s = ptr;
    size_t size = sizeof(struct ractor_selector);
    if (s->ports) {
        size += st_memsize(s->ports);
    }
    return size;
}

static const rb_data_type_t ractor_selector_data_type = {
    "ractor/selector",
    {
        ractor_selector_mark,
        ractor_selector_free,
        ractor_selector_memsize,
        NULL, // update
    },
    0, 0, RUBY_TYPED_THREAD_SAFE_FREE | RUBY_TYPED_WB_PROTECTED,
};

static struct ractor_selector *
RACTOR_SELECTOR_PTR(VALUE selv)
{
    VM_ASSERT(rb_typeddata_is_kind_of(selv, &ractor_selector_data_type));
    return (struct ractor_selector *)DATA_PTR(selv);
}

// Ractor::Selector.new

static VALUE
ractor_selector_create(VALUE klass)
{
    struct ractor_selector *s;
    VALUE selv = TypedData_Make_Struct(klass, struct ractor_selector, &ractor_selector_data_type, s);
    s->ports = st_init_numtable(); // TODO
    return selv;
}

// Ractor::Selector#add(r)

/*
 * call-seq:
 *   add(ractor) -> ractor
 *
 * Adds _ractor_ to +self+.  Raises an exception if _ractor_ is already added.
 * Returns _ractor_.
 */
static VALUE
ractor_selector_add(VALUE selv, VALUE rpv)
{
    if (!ractor_port_p(rpv)) {
        rb_raise(rb_eArgError, "Not a Ractor::Port object");
    }

    struct ractor_selector *s = RACTOR_SELECTOR_PTR(selv);
    const struct ractor_port *rp = RACTOR_PORT_PTR(rpv);

    if (st_lookup(s->ports, (st_data_t)rpv, NULL)) {
        rb_raise(rb_eArgError, "already added");
    }

    st_insert(s->ports, (st_data_t)rpv, (st_data_t)rp);
    RB_OBJ_WRITTEN(selv, Qundef, rpv);

    return selv;
}

// Ractor::Selector#remove(r)

/* call-seq:
 *   remove(ractor) -> ractor
 *
 * Removes _ractor_ from +self+.  Raises an exception if _ractor_ is not added.
 * Returns the removed _ractor_.
 */
static VALUE
ractor_selector_remove(VALUE selv, VALUE rpv)
{
    if (!ractor_port_p(rpv)) {
        rb_raise(rb_eArgError, "Not a Ractor::Port object");
    }

    struct ractor_selector *s = RACTOR_SELECTOR_PTR(selv);

    if (!st_lookup(s->ports, (st_data_t)rpv, NULL)) {
        rb_raise(rb_eArgError, "not added yet");
    }

    st_delete(s->ports, (st_data_t *)&rpv, NULL);

    return selv;
}

// Ractor::Selector#clear

/*
 * call-seq:
 *   clear -> self
 *
 * Removes all ractors from +self+.  Raises +self+.
 */
static VALUE
ractor_selector_clear(VALUE selv)
{
    struct ractor_selector *s = RACTOR_SELECTOR_PTR(selv);
    st_clear(s->ports);
    return selv;
}

/*
 * call-seq:
 *  empty? -> true or false
 *
 * Returns +true+ if no ractor is added.
 */
static VALUE
ractor_selector_empty_p(VALUE selv)
{
    struct ractor_selector *s = RACTOR_SELECTOR_PTR(selv);
    return s->ports->num_entries == 0 ? Qtrue : Qfalse;
}

// Ractor::Selector#wait

struct ractor_selector_wait_data {
    rb_ractor_t *cr;
    rb_execution_context_t *ec;
    bool found;
    VALUE v;
    VALUE rpv;
};

static int
ractor_selector_wait_i(st_data_t key, st_data_t val, st_data_t data)
{
    struct ractor_selector_wait_data *p = (struct ractor_selector_wait_data *)data;
    const struct ractor_port *rp = (const struct ractor_port *)val;

    VALUE v = ractor_try_receive(p->ec, p->cr, rp);

    if (v != Qundef) {
        p->found = true;
        p->v = v;
        p->rpv = (VALUE)key;
        return ST_STOP;
    }
    else {
        return ST_CONTINUE;
    }
}

static VALUE
ractor_selector__wait(rb_execution_context_t *ec, VALUE selector)
{
    rb_ractor_t *cr = rb_ec_ractor_ptr(ec);
    struct ractor_selector *s = RACTOR_SELECTOR_PTR(selector);

    struct ractor_selector_wait_data data = {
        .ec = ec,
        .cr = cr,
        .found = false,
    };

    while (1) {
        st_foreach(s->ports, ractor_selector_wait_i, (st_data_t)&data);

        if (data.found) {
            return rb_ary_new_from_args(2, data.rpv, data.v);
        }

        ractor_wait_receive(ec, cr);
    }
}

/*
 * call-seq:
 *  wait(receive: false, yield_value: undef, move: false) -> [ractor, value]
 *
 * Waits until any ractor in _selector_ can be active.
 */
static VALUE
ractor_selector_wait(VALUE selector)
{
    return ractor_selector__wait(GET_EC(), selector);
}

static VALUE
ractor_selector_new(int argc, VALUE *ractors, VALUE klass)
{
    VALUE selector = ractor_selector_create(klass);

    for (int i=0; i<argc; i++) {
        ractor_selector_add(selector, ractors[i]);
    }

    return selector;
}

static VALUE
ractor_select_internal(rb_execution_context_t *ec, VALUE self, VALUE ports)
{
    VALUE selector = ractor_selector_new(RARRAY_LENINT(ports), (VALUE *)RARRAY_CONST_PTR(ports), rb_cRactorSelector);
    VALUE result = ractor_selector__wait(ec, selector);

    RB_GC_GUARD(selector);
    RB_GC_GUARD(ports);
    return result;
}

#ifndef USE_RACTOR_SELECTOR
#define USE_RACTOR_SELECTOR 0
#endif

RUBY_SYMBOL_EXPORT_BEGIN
void rb_init_ractor_selector(void);
RUBY_SYMBOL_EXPORT_END

/*
 * Document-class: Ractor::Selector
 * :nodoc: currently
 *
 * Selects multiple Ractors to be activated.
 */
void
rb_init_ractor_selector(void)
{
    rb_cRactorSelector = rb_define_class_under(rb_cRactor, "Selector", rb_cObject);
    rb_undef_alloc_func(rb_cRactorSelector);

    rb_define_singleton_method(rb_cRactorSelector, "new", ractor_selector_new , -1);
    rb_define_method(rb_cRactorSelector, "add", ractor_selector_add, 1);
    rb_define_method(rb_cRactorSelector, "remove", ractor_selector_remove, 1);
    rb_define_method(rb_cRactorSelector, "clear", ractor_selector_clear, 0);
    rb_define_method(rb_cRactorSelector, "empty?", ractor_selector_empty_p, 0);
    rb_define_method(rb_cRactorSelector, "wait", ractor_selector_wait, 0);
}

static void
Init_RactorPort(void)
{
    rb_cRactorPort = rb_define_class_under(rb_cRactor, "Port", rb_cObject);
    rb_define_alloc_func(rb_cRactorPort, ractor_port_alloc);
    rb_define_method(rb_cRactorPort, "initialize", ractor_port_initialize, 0);
    rb_define_method(rb_cRactorPort, "initialize_copy", ractor_port_initialize_copy, 1);

#if USE_RACTOR_SELECTOR
    rb_init_ractor_selector();
#endif
}
