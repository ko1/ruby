// Ractor implementation

#include "ruby/ruby.h"
#include "ruby/thread.h"
#include "ruby/thread_native.h"
#include "vm_core.h"
#include "ractor.h"

static VALUE rb_cRactor;
static VALUE rb_eRactorError;
static VALUE rb_eRactorRemoteError;
static VALUE rb_eRactorMovedError;
static VALUE rb_eRactorClosedError;

static VALUE rb_cRactorMovedObject;

enum rb_ractor_basket_type {
    basket_type_none,
    basket_type_shareable,
    basket_type_copy_marshal,
    basket_type_copy_custom,
    basket_type_move,
    basket_type_exception,
};

struct rb_ractor_basket {
    int type;
    VALUE v;
    VALUE sender;
};

struct rb_ractor_queue {
    struct rb_ractor_basket *baskets;
    int cnt;
    int size;
};

struct rb_ractor_waiting_list {
    int cnt;
    int size;
    rb_ractor_t **ractors;
};

struct rb_ractor_struct {
    rb_nativethread_lock_t lock;
#if RACTOR_CHECK_MODE > 0
    VALUE locked_by;
#endif

    struct rb_ractor_queue  incoming_queue;

    bool incoming_port_closed;
    bool outgoing_port_closed;

    struct rb_ractor_waiting_list taking_ractors;

    struct ractor_wait {
        enum ractor_wait_status {
            wait_none     = 0x00,
            wait_recving  = 0x01,
            wait_taking   = 0x02,
            wait_yielding = 0x04,
        } status;

        enum ractor_wakeup_status {
            wakeup_none,
            wakeup_by_send,
            wakeup_by_yield,
            wakeup_by_take,
            wakeup_by_close,
            wakeup_by_interrupt,
        } wakeup_status;

        struct rb_ractor_basket taken_basket;
        struct rb_ractor_basket yielded_basket;

        rb_nativethread_cond_t cond;
    } wait;

    VALUE running_thread; // TODO

    // identity
    VALUE self;
    uint32_t id;
    VALUE name;
    VALUE loc;
}; // rb_ractor_t is defined in vm_core.h

static void
ASSERT_ractor_unlocking(rb_ractor_t *r)
{
#if RACTOR_CHECK_MODE > 0
    if (r->locked_by == GET_RACTOR()->self) {
        rb_bug("recursive ractor locking");
    }
#endif
}

static void
ASSERT_ractor_locking(rb_ractor_t *r)
{
#if RACTOR_CHECK_MODE > 0
    if (r->locked_by != GET_RACTOR()->self) {
        rp(r->locked_by);
        rb_bug("ractor lock is not acquired.");
    }
#endif
}

static void
ractor_lock(rb_ractor_t *r)
{
    ASSERT_ractor_unlocking(r);

    rb_native_mutex_lock(&r->lock);

#if RACTOR_CHECK_MODE > 0
    r->locked_by = GET_RACTOR()->self;
#endif
}

static void
ractor_lock_self(rb_ractor_t *cr)
{
    VM_ASSERT(cr->locked_by != cr->self);
    rb_native_mutex_lock(&cr->lock);
#if RACTOR_CHECK_MODE > 0
    cr->locked_by = cr->self;
#endif
}

static void
ractor_unlock(rb_ractor_t *r)
{
    ASSERT_ractor_locking(r);
#if RACTOR_CHECK_MODE > 0
    r->locked_by = Qnil;
#endif
    rb_native_mutex_unlock(&r->lock);
}

static void
ractor_unlock_self(rb_ractor_t *cr)
{
    VM_ASSERT(cr->locked_by == cr->self);
#if RACTOR_CHECK_MODE > 0
    cr->locked_by = Qnil;
#endif
    rb_native_mutex_unlock(&cr->lock);
}

static void
ractor_cond_wait(rb_ractor_t *r)
{
#if RACTOR_CHECK_MODE > 0
    VALUE locked_by = r->locked_by;
    r->locked_by = Qnil;
#endif
    rb_native_cond_wait(&r->wait.cond, &r->lock);

#if RACTOR_CHECK_MODE > 0
    r->locked_by = locked_by;
#endif
}

static void
ractor_queue_mark(struct rb_ractor_queue *rq)
{
    for (int i=0; i<rq->cnt; i++) {
        rb_gc_mark(rq->baskets[i].v);
        rb_gc_mark(rq->baskets[i].sender);
    }
}

static void
ractor_mark(void *ptr)
{
    rb_ractor_t *r = (rb_ractor_t *)ptr;

    ractor_queue_mark(&r->incoming_queue);
    rb_gc_mark(r->wait.taken_basket.v);
    rb_gc_mark(r->wait.taken_basket.sender);
    rb_gc_mark(r->wait.yielded_basket.v);
    rb_gc_mark(r->wait.yielded_basket.sender);
    rb_gc_mark(r->running_thread);
    rb_gc_mark(r->loc);
    rb_gc_mark(r->name);
}

static void
ractor_queue_free(struct rb_ractor_queue *rq)
{
    ruby_xfree(rq->baskets);
}

static void
ractor_waiting_list_free(struct rb_ractor_waiting_list *wl)
{
    ruby_xfree(wl->ractors);
}

static void
ractor_free(void *ptr)
{
    rb_ractor_t *r = (rb_ractor_t *)ptr;
    rb_native_mutex_destroy(&r->lock);
    rb_native_cond_destroy(&r->wait.cond);
    ractor_queue_free(&r->incoming_queue);
    ractor_waiting_list_free(&r->taking_ractors);
}

static size_t
ractor_queue_memsize(const struct rb_ractor_queue *rq)
{
    return sizeof(struct rb_ractor_basket) * rq->size;
}

static size_t
ractor_waiting_list_memsize(const struct rb_ractor_waiting_list *wl)
{
    return sizeof(rb_ractor_t *) * wl->size;
}

static size_t
ractor_memsize(const void *ptr)
{
    rb_ractor_t *r = (rb_ractor_t *)ptr;

    // TODO
    return sizeof(rb_ractor_t) +
      ractor_queue_memsize(&r->incoming_queue) +
      ractor_waiting_list_memsize(&r->taking_ractors);
}

static const rb_data_type_t ractor_data_type = {
    "ractor",
    {
        ractor_mark,
	ractor_free,
        ractor_memsize,
        NULL, // update
    },
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY /* | RUBY_TYPED_WB_PROTECTED */
};

bool
rb_ractor_p(VALUE gv)
{
    if (rb_typeddata_is_kind_of(gv, &ractor_data_type)) {
        return true;
    }
    else {
        return false;
    }
}

static inline rb_ractor_t *
RACTOR_PTR(VALUE self)
{
    VM_ASSERT(rb_ractor_p(self));

    rb_ractor_t *r = DATA_PTR(self);
    // TODO: check
    return r;
}

uint32_t
rb_ractor_id(const rb_ractor_t *g)
{
    return g->id;
}

static uint32_t ractor_last_id;

#if RACTOR_CHECK_MODE > 0
MJIT_FUNC_EXPORTED uint32_t
rb_ractor_current_id(void)
{
    if (GET_THREAD()->ractor == NULL) {
        return 1; // main ractor
    }
    else {
        return GET_RACTOR()->id;
    }
}
#endif

static void
ractor_queue_setup(struct rb_ractor_queue *rq)
{
    rq->size = 2;
    rq->cnt = 0;
    rq->baskets = ALLOC_N(struct rb_ractor_basket, rq->size);
}

static bool
ractor_queue_empty_p(rb_ractor_t *r, struct rb_ractor_queue *rq)
{
    ASSERT_ractor_locking(r);
    return rq->cnt == 0;
}

static bool
ractor_queue_deq(rb_ractor_t *r, struct rb_ractor_queue *rq, struct rb_ractor_basket *basket)
{
    bool b;

    ractor_lock(r);
    {
        if (!ractor_queue_empty_p(r, rq)) {
            // TODO: use good Queue data structure
            *basket = rq->baskets[0];
            rq->cnt--;
            for (int i=0; i<rq->cnt; i++) {
                rq->baskets[i] = rq->baskets[i+1];
            }
            b = true;
        }
        else {
            b = false;
        }
    }
    ractor_unlock(r);

    return b;
}

static void
ractor_queue_enq(rb_ractor_t *r, struct rb_ractor_queue *rq, struct rb_ractor_basket *basket)
{
    ASSERT_ractor_locking(r);

    if (rq->size <= rq->cnt) {
        rq->size *= 2;
        REALLOC_N(rq->baskets, struct rb_ractor_basket, rq->size);
    }
    rq->baskets[rq->cnt++] = *basket;
    // fprintf(stderr, "%s %p->cnt:%d\n", __func__, rq, rq->cnt);
}

VALUE rb_newobj_with(VALUE src); // gc.c

static VALUE
ractor_moving_new(VALUE obj)
{
    // create moving object
    VALUE v = rb_newobj_with(obj);

    // invalidate src object
    struct RVALUE {
        VALUE flags;
        VALUE klass;
        VALUE v1;
        VALUE v2;
        VALUE v3;
    } *rv = (void *)obj;

    rv->klass = rb_cRactorMovedObject;
    rv->v1 = 0;
    rv->v2 = 0;
    rv->v3 = 0;

    // TODO: record moved location
    // TOOD: check flags for each data types

    return v;
}

static VALUE
ractor_move_shallow_copy(VALUE obj)
{
    if (rb_ractor_shareable_p(obj)) {
        return obj;
    }
    else {
        switch (BUILTIN_TYPE(obj)) {
          case T_STRING:
          case T_FILE:
            if (!FL_TEST_RAW(obj, RUBY_FL_EXIVAR)) {
                return ractor_moving_new(obj);
            }
            break;
          case T_ARRAY:
            if (!FL_TEST_RAW(obj, RUBY_FL_EXIVAR)) {
                VALUE ary = ractor_moving_new(obj);
                long len = RARRAY_LEN(ary);
                for (long i=0; i<len; i++) {
                    VALUE e = RARRAY_AREF(ary, i);
                    RARRAY_ASET(ary, i, ractor_move_shallow_copy(e)); // confirm WB
                }
                return ary;
            }
            break;
          default:
            break;
        }

        rb_raise(rb_eRactorError, "can't move this this kind of object:%"PRIsVALUE, obj);
    }
}

static VALUE
ractor_moved_setup(VALUE obj)
{
#if RACTOR_CHECK_MODE
    switch (BUILTIN_TYPE(obj)) {
      case T_STRING:
      case T_FILE:
        rb_ractor_setup_belonging(obj);
        break;
      case T_ARRAY:
        rb_ractor_setup_belonging(obj);
        long len = RARRAY_LEN(obj);
        for (long i=0; i<len; i++) {
            VALUE e = RARRAY_AREF(obj, i);
            if (!rb_ractor_shareable_p(e)) {
                ractor_moved_setup(e);
            }
        }
        break;
      default:
        rb_bug("unreachable");
    }
#endif
    return obj;
}

static void
ractor_move_setup(struct rb_ractor_basket *b, VALUE obj)
{
    if (rb_ractor_shareable_p(obj)) {
        b->type = basket_type_shareable;
        b->v = obj;
    }
    else {
        b->type = basket_type_move;
        b->v = ractor_move_shallow_copy(obj);
        return;
    }
}

static void
ractor_basket_clear(struct rb_ractor_basket *b)
{
    b->type = basket_type_none;
    b->v = Qfalse;
    b->sender = Qfalse;
}

static VALUE
ractor_basket_accept(struct rb_ractor_basket *b)
{
    VALUE v;
    switch (b->type) {
      case basket_type_shareable:
        VM_ASSERT(rb_ractor_shareable_p(b->v));
        v = b->v;
        break;
      case basket_type_copy_marshal:
        v = rb_marshal_load(b->v);
        break;
      case basket_type_exception:
        {
            VALUE cause = rb_marshal_load(b->v);
            VALUE err = rb_exc_new_cstr(rb_eRactorRemoteError, "thrown by remote Ractor.");
            rb_ivar_set(err, rb_intern("@ractor"), b->sender);
            ractor_basket_clear(b);
            rb_ec_setup_exception(NULL, err, cause);
            rb_exc_raise(err);
        }
        // unreachable
      case basket_type_move:
        v = ractor_moved_setup(b->v);
        break;
      default:
        rb_bug("unreachable");
    }
    ractor_basket_clear(b);
    return v;
}

static void
ractor_copy_setup(struct rb_ractor_basket *b, VALUE obj)
{
    if (rb_ractor_shareable_p(obj)) {
        b->type = basket_type_shareable;
        b->v = obj;
    }
    else {
#if 0
        // TODO: consider custom copy protocol
        switch (BUILTIN_TYPE(obj)) {
            
        }
#endif
        b->v = rb_marshal_dump(obj, Qnil);
        b->type = basket_type_copy_marshal;
    }
}

static VALUE
ractor_try_recv(rb_execution_context_t *ec, rb_ractor_t *r)
{
    struct rb_ractor_queue *rq = &r->incoming_queue;
    struct rb_ractor_basket basket;

    if (ractor_queue_deq(r, rq, &basket) == false) {
        if (r->incoming_port_closed) {
            rb_raise(rb_eRactorClosedError, "The incoming port is already closed");
        }
        else {
            return Qundef;
        }
    }

    return ractor_basket_accept(&basket);
}

static void *
ractor_sleep_wo_gvl(void *ptr)
{
    rb_ractor_t *cr = ptr;
    ractor_lock_self(cr);
    VM_ASSERT(cr->wait.status != wait_none);
    if (cr->wait.wakeup_status == wakeup_none) {
        ractor_cond_wait(cr);
    }
    cr->wait.status = wait_none;
    ractor_unlock_self(cr);
    return NULL;
}

static void
ractor_sleep_interrupt(void *ptr)
{
    rb_ractor_t *r = ptr;

    ractor_lock(r);
    if (r->wait.wakeup_status == wakeup_none) {
        r->wait.wakeup_status = wakeup_by_interrupt;
        rb_native_cond_signal(&r->wait.cond);
    }
    ractor_unlock(r);
}

const char *
wait_status_str(enum ractor_wait_status wait_status)
{
    switch ((int)wait_status) {
      case wait_none: return "none";
      case wait_recving: return "recving";
      case wait_taking: return "taking";
      case wait_yielding: return "yielding";
      case wait_recving|wait_taking: return "recving|taking";
      case wait_recving|wait_yielding: return "recving|yielding";
      case wait_taking|wait_yielding: return "taking|yielding";
      case wait_recving|wait_taking|wait_yielding: return "recving|taking|yielding";
    }
    rb_bug("unrechable");
}

const char *
wakeup_status_str(enum ractor_wakeup_status wakeup_status)
{
    switch (wakeup_status) {
      case wakeup_none: return "none";
      case wakeup_by_send: return "by_send";
      case wakeup_by_yield: return "by_yield";
      case wakeup_by_take: return "by_take";
      case wakeup_by_close: return "by_close";
      case wakeup_by_interrupt: return "by_interrupt";
    }
    rb_bug("unrechable");
}

static void
ractor_sleep(rb_execution_context_t *ec, rb_ractor_t *cr)
{
    VM_ASSERT(GET_RACTOR() == cr);
    VM_ASSERT(cr->wait.status != wait_none);
    // fprintf(stderr, "%s  r:%p status:%s, wakeup_status:%s\n", __func__, cr,
    //                 wait_status_str(cr->wait.status), wakeup_status_str(cr->wait.wakeup_status));

    ractor_unlock(cr);
    rb_thread_call_without_gvl(ractor_sleep_wo_gvl, cr,
                               ractor_sleep_interrupt, cr);
    ractor_lock(cr);
}

static bool
ractor_wakeup(rb_ractor_t *r, enum ractor_wait_status wait_status, enum ractor_wakeup_status wakeup_status)
{
    ASSERT_ractor_locking(r);

    // fprintf(stderr, "%s r:%p status:%s/%s wakeup_status:%s/%s\n", __func__, r,
    //         wait_status_str(r->wait.status), wait_status_str(wait_status),
    //         wakeup_status_str(r->wait.wakeup_status), wakeup_status_str(wakeup_status));

    if ((r->wait.status & wait_status) &&
        r->wait.wakeup_status == wakeup_none) {
        r->wait.wakeup_status = wakeup_status;
        rb_native_cond_signal(&r->wait.cond);
        return true;
    }
    else {
        return false;
    }
}

static void
ractor_waiting_list_add(rb_ractor_t *r, struct rb_ractor_waiting_list *wl, rb_ractor_t *wr)
{
    // wl is belong to r
    ractor_lock(r);
    {
        for (int i=0; i<wl->cnt; i++) {
            if (wl->ractors[i] == wr) {
                // TODO: make it clean code.
                rb_native_mutex_unlock(&r->lock);
                rb_raise(rb_eRuntimeError, "Already another thread of same ractor is waiting.");
            }
        }

        if (wl->size == 0) {
            wl->size = 1;
            wl->ractors = ALLOC_N(rb_ractor_t *, wl->size);
        }
        else if (wl->size <= wl->cnt + 1) {
            wl->size *= 2;
            REALLOC_N(wl->ractors, rb_ractor_t *, wl->size);
        }
        wl->ractors[wl->cnt++] = wr;
        // fprintf(stderr, "cnt:%d size:%d\n", rq->waiting.cnt, rq->waiting.size);
    }
    ractor_unlock(r);
}

static void
ractor_waiting_list_del(rb_ractor_t *r, struct rb_ractor_waiting_list *wl, rb_ractor_t *wr)
{
    ractor_lock(r);
    {
        int pos = -1;
        for (int i=0; i<wl->cnt; i++) {
            if (wl->ractors[i] == wr) {
                pos = i;
                break;
            }
        }
        if (pos >= 0) { // found
            wl->cnt--;
            for (int i=pos; i<wl->cnt; i++) {
                wl->ractors[i] = wl->ractors[i+1];
            }
        }
    }
    ractor_unlock(r);
}

static rb_ractor_t *
ractor_waiting_list_shift(rb_ractor_t *r, struct rb_ractor_waiting_list *wl)
{
    VM_ASSERT(&r->taking_ractors == wl);
    rb_ractor_t *tr;

    ractor_lock(r);
    {
        if (wl->cnt > 0) {
            tr = wl->ractors[0];
            for (int i=1; i<wl->cnt; i++) {
                wl->ractors[i-1] = wl->ractors[i];
            }
            wl->cnt--;
        }
        else {
            tr = NULL;
        }
    }
    ractor_unlock(r);

    return tr;
}

static VALUE
ractor_recv(rb_execution_context_t *ec, rb_ractor_t *r)
{
    VM_ASSERT(r == rb_ec_ractor_ptr(ec));
    VALUE v;

    while ((v = ractor_try_recv(ec, r)) == Qundef) {
        ractor_lock(r);
        {
            if (ractor_queue_empty_p(r, &r->incoming_queue)) {
                VM_ASSERT(r->wait.status == wait_none);
                VM_ASSERT(r->wait.wakeup_status == wakeup_none);
                r->wait.status = wait_recving;

                ractor_sleep(ec, r);

                r->wait.wakeup_status = wakeup_none;
            }
        }
        ractor_unlock(r);
    }

    return v;
}

static void
ractor_send_basket(rb_execution_context_t *ec, rb_ractor_t *r, struct rb_ractor_basket *b)
{
    bool closed = false;
    struct rb_ractor_queue *rq = &r->incoming_queue;

    ractor_lock(r);
    {
        if (r->incoming_port_closed) {
            closed = true;
        }
        else {
            ractor_queue_enq(r, rq, b);
            if (ractor_wakeup(r, wait_recving, wakeup_by_send)) {
                VM_ASSERT(rq->cnt == 1);
            }
        }
    }
    ractor_unlock(r);

    if (closed) {
        rb_raise(rb_eRactorClosedError, "The incoming-port is already closed");
    }
}

static void
ractor_basket_setup(rb_execution_context_t *ec, struct rb_ractor_basket *basket, VALUE obj, VALUE move)
{
    basket->sender = rb_ec_ractor_ptr(ec)->self;

    if (!RTEST(move)) {
        ractor_copy_setup(basket, obj);
    }
    else {
        ractor_move_setup(basket, obj);
    }
}

static VALUE
ractor_send(rb_execution_context_t *ec, rb_ractor_t *r, VALUE obj, VALUE move)
{
    struct rb_ractor_basket basket;
    ractor_basket_setup(ec, &basket, obj, move);
    ractor_send_basket(ec, r, &basket);
    return r->self;
}

static VALUE
ractor_try_take(rb_execution_context_t *ec, rb_ractor_t *r)
{
    struct rb_ractor_basket basket = {
        .type = basket_type_none,
    };
    bool closed = false;

    ractor_lock(r);
    {
        if (ractor_wakeup(r, wait_yielding, wakeup_by_take)) {
            VM_ASSERT(r->wait.yielded_basket.type != basket_type_none);
            basket = r->wait.yielded_basket;
            ractor_basket_clear(&r->wait.yielded_basket);
        }
        else if (r->outgoing_port_closed) {
            closed = true;
        }
    }
    ractor_unlock(r);

    if (basket.type == basket_type_none) {
        if (closed) {
            rb_raise(rb_eRactorClosedError, "The outgoing-port is already closed");
        }
        else {
            return Qundef;
        }
    }
    else {
        return ractor_basket_accept(&basket);
    }
}

static bool
ractor_try_yield(rb_execution_context_t *ec, rb_ractor_t *cr, VALUE obj, bool move, bool exc)
{
    rb_ractor_t *r;
  retry:
    r = ractor_waiting_list_shift(cr, &cr->taking_ractors);

    if (r) {
        bool retry = false;

        ractor_lock(r);
        {
            if (ractor_wakeup(r, wait_taking, wakeup_by_yield)) {
                VM_ASSERT(r->wait.taken_basket.type == basket_type_none);
                ractor_basket_setup(ec, &r->wait.taken_basket, obj, move);
                if (exc) {
                    r->wait.taken_basket.type = basket_type_exception;
                }
            }
            else {
                retry = true;
            }
        }
        ractor_unlock(r);

        if (retry) {
            // get candidate take-waiting ractor, but already woke up by another reason.
            // retry to check another ractor.
            goto retry;
        }
        else {
            return true;
        }
    }
    else {
        return false;
    }
}

// select(r1, r2, r3, receive: true, yield: obj)
static VALUE
ractor_select(rb_execution_context_t *ec, const VALUE *rs, int alen, VALUE yielded_value, bool move, VALUE *ret_r)
{
    rb_ractor_t *cr = rb_ec_ractor_ptr(ec);
    VALUE crv = cr->self;
    VALUE ret;
    int i;
    enum ractor_wait_status wait_status = 0;

    bool yield_p = (yielded_value != Qundef) ? true : false;

    struct ractor_select_action {
        enum ractor_select_action_type {
            ractor_select_action_take,
            ractor_select_action_recv,
            ractor_select_action_yield,
        } type;
        VALUE v;
    } *actions = ALLOCA_N(struct ractor_select_action, alen + (yield_p ? 1 : 0));

    VM_ASSERT(cr->wait.status == wait_none);
    VM_ASSERT(cr->wait.wakeup_status == wakeup_none);
    VM_ASSERT(cr->wait.taken_basket.type == basket_type_none);
    VM_ASSERT(cr->wait.yielded_basket.type == basket_type_none);

    // setup actions
    for (i=0; i<alen; i++) {
        VALUE v = rs[i];

        if (v == crv) {
            actions[i].type = ractor_select_action_recv;
            actions[i].v = Qnil;
            wait_status |= wait_recving;
        }
        else if (rb_ractor_p(v)) {
            actions[i].type = ractor_select_action_take;
            actions[i].v = v;
            wait_status |= wait_taking;
        }
        else {
            rb_raise(rb_eArgError, "It should be ractor objects");
        }
    }
    rs = NULL;
    if (yield_p) {
        actions[i].type = ractor_select_action_yield;
        actions[i].v = yielded_value;
        wait_status |= wait_yielding;
        alen++;
    }

    // TODO: shuffle actions

    while (1) {
        // try actions
        for (i=0; i<alen; i++) {
            VALUE v, rv;
            switch (actions[i].type) {
              case ractor_select_action_take:
                rv = actions[i].v;
                v = ractor_try_take(ec, RACTOR_PTR(rv));
                if (v != Qundef) {
                    *ret_r = rv;
                    ret = v;
                    goto cleanup;
                }
                break;
              case ractor_select_action_recv:
                v = ractor_try_recv(ec, cr);
                if (v != Qundef) {
                    *ret_r = ID2SYM(rb_intern("recv"));
                    ret = v;
                    goto cleanup;
                }
                break;
              case ractor_select_action_yield:
                if (ractor_try_yield(ec, cr, actions[i].v, move, false)) {
                    *ret_r = ID2SYM(rb_intern("yield"));
                    ret = Qnil;
                    goto cleanup;
                }
                break;
            }
        }

        // setup waiting status
        ractor_lock(cr);
        {
            VM_ASSERT(cr->wait.status == wait_none);
            VM_ASSERT(cr->wait.wakeup_status == wakeup_none);

            cr->wait.status = wait_status;
        }
        ractor_unlock(cr);

        // prepare waiting
        for (i=0; i<alen; i++) {
            rb_ractor_t *r;
            switch (actions[i].type) {
              case ractor_select_action_take:
                r = RACTOR_PTR(actions[i].v);
                ractor_waiting_list_add(r, &r->taking_ractors, cr);
                break;
              case ractor_select_action_yield:
                if (cr->wait.yielded_basket.type == basket_type_none) {
                    ractor_basket_setup(ec, &cr->wait.yielded_basket, yielded_value, move);
                }
                break;
              case ractor_select_action_recv:
                break;
            }
        }

        // wait
        ractor_lock(cr);
        {
            if (cr->wait.wakeup_status == wakeup_none) {
                VM_ASSERT(cr->wait.status != wait_none);
                ractor_sleep(ec, cr);
            }
        }
        ractor_unlock(cr);

        // cleanup waiting
        for (i=0; i<alen; i++) {
            rb_ractor_t *r;
            switch (actions[i].type) {
              case ractor_select_action_take:
                r = RACTOR_PTR(actions[i].v);
                ractor_waiting_list_del(r, &r->taking_ractors, cr);
                break;
              case ractor_select_action_recv:
              case ractor_select_action_yield:
                break;
            }
        }

        // check results
        enum ractor_wakeup_status wakeup_status = cr->wait.wakeup_status;
        cr->wait.wakeup_status = wakeup_none;

        switch (wakeup_status) {
          case wakeup_none:
            // OK. something happens.
            // retry loop.
            break;
          case wakeup_by_send:
            // OK.
            // retry loop and try_recv will succss.
            break;
          case wakeup_by_yield:
            // take was succeeded!
            // cr.wait.taken_basket contains passed block
            VM_ASSERT(cr->wait.taken_basket.type != basket_type_none);
            *ret_r = cr->wait.taken_basket.sender;
            VM_ASSERT(rb_ractor_p(*ret_r));
            ret = ractor_basket_accept(&cr->wait.taken_basket);
            goto cleanup;
          case wakeup_by_take:
            *ret_r = ID2SYM(rb_intern("yield"));
            ret = Qnil;
            goto cleanup;
          case wakeup_by_close:
            // OK.
            // retry loop and will get CloseError.
            break;
          case wakeup_by_interrupt:
            goto cleanup;
        }
    }

  cleanup:
    if (cr->wait.yielded_basket.type != basket_type_none) {
        ractor_basket_clear(&cr->wait.yielded_basket);
    }

    VM_ASSERT(cr->wait.status == wait_none);
    VM_ASSERT(cr->wait.wakeup_status == wakeup_none);
    VM_ASSERT(cr->wait.taken_basket.type == basket_type_none);
    VM_ASSERT(cr->wait.yielded_basket.type == basket_type_none);

    RUBY_VM_CHECK_INTS(ec);
    return ret;
}

static VALUE
ractor_yield(rb_execution_context_t *ec, rb_ractor_t *r, VALUE obj, VALUE move)
{
    VALUE ret_r;
    ractor_select(ec, NULL, 0, obj, RTEST(move) ? true : false, &ret_r);
    return Qnil;
}

static VALUE
ractor_take(rb_execution_context_t *ec, rb_ractor_t *r)
{
    VALUE ret_r;
    VALUE v = ractor_select(ec, &r->self, 1, Qundef, false, &ret_r);
    return v;
}

static VALUE
ractor_close_incoming(rb_execution_context_t *ec, rb_ractor_t *r)
{
    VALUE prev;

    ractor_lock(r);
    {
        if (!r->incoming_port_closed) {
            prev = Qfalse;
            r->incoming_port_closed = true;
            if (ractor_wakeup(r, wait_recving, wakeup_by_close)) {
                VM_ASSERT(r->incoming_queue.cnt == 0);
            }
        }
        else {
            prev = Qtrue;
        }
    }
    ractor_unlock(r);
    return prev;
}

static VALUE
ractor_close_outgoing(rb_execution_context_t *ec, rb_ractor_t *r)
{
    VALUE prev;

    ractor_lock(r);
    {
        if (!r->outgoing_port_closed) {
            prev = Qfalse;
            r->outgoing_port_closed = true;
        }
        else {
            prev = Qtrue;
        }
    }
    ractor_unlock(r);

    // wakeup all taking ractors
    rb_ractor_t *taking_ractor;
    while ((taking_ractor = ractor_waiting_list_shift(r, &r->taking_ractors)) != NULL) {
        ractor_lock(taking_ractor);
        ractor_wakeup(taking_ractor, wait_taking, wakeup_by_close);
        ractor_unlock(taking_ractor);
    }
    return prev;
}

// creation/termination

static uint32_t
ractor_next_id(void)
{
    // TODO: lock
    return ++ractor_last_id;
}

static void
ractor_setup(rb_ractor_t *r)
{
    ractor_queue_setup(&r->incoming_queue);
    rb_native_mutex_initialize(&r->lock);
    rb_native_cond_initialize(&r->wait.cond);
}

static VALUE
ractor_alloc(VALUE klass)
{
    rb_ractor_t *r;
    VALUE rv = TypedData_Make_Struct(klass, rb_ractor_t, &ractor_data_type, r);
    FL_SET_RAW(rv, RUBY_FL_SHAREABLE);

    // namig
    r->id = ractor_next_id();
    r->loc = Qnil;
    r->name = Qnil;
    r->self = rv;
    ractor_setup(r);
    return rv;
}

rb_ractor_t *
rb_ractor_main_alloc(void)
{
    rb_ractor_t *r = ruby_mimmalloc(sizeof(rb_ractor_t));
    if (r == NULL) {
	fprintf(stderr, "[FATAL] failed to allocate memory for main ractor\n");
        exit(EXIT_FAILURE);
    }
    MEMZERO(r, rb_ractor_t, 1);
    r->id = ++ractor_last_id;
    r->loc = Qnil;
    r->name = Qnil;

    return r;
}

void
rb_ractor_main_setup(rb_ractor_t *r)
{
    r->self = TypedData_Wrap_Struct(rb_cRactor, &ractor_data_type, r);
    FL_SET_RAW(r->self, RUBY_FL_SHAREABLE);
    ractor_setup(r);
}


VALUE
rb_ractor_self(const rb_ractor_t *r)
{
    return r->self;
}

MJIT_FUNC_EXPORTED int
rb_ractor_main_p(void)
{
    rb_execution_context_t *ec = GET_EC();
    return rb_ec_ractor_ptr(ec) == rb_ec_vm_ptr(ec)->main_ractor;
}

static VALUE
ractor_create(rb_execution_context_t *ec, VALUE self,
             VALUE args, VALUE block, VALUE loc, VALUE name)
{
    VALUE rv = ractor_alloc(self);
    rb_ractor_t *r = RACTOR_PTR(rv);
    r->running_thread = rb_thread_create_ractor(r, args, block);
    r->loc = loc;
    r->name = name;
    return rv;
}

static void
ractor_atexit_yield(rb_execution_context_t *ec, rb_ractor_t *cr, VALUE v, bool exc)
{
    ASSERT_ractor_unlocking(cr);

    if (ractor_try_yield(ec, cr, v, false, exc)) {
        return; // OK
    }

    VM_ASSERT(cr->wait.status == wait_none);
    VM_ASSERT(cr->wait.wakeup_status == wakeup_none);
    VM_ASSERT(cr->wait.yielded_basket.type == basket_type_none);
    ractor_basket_setup(ec, &cr->wait.yielded_basket, v, false);

    ractor_lock(cr);
    {
        VM_ASSERT(cr->wait.status == wait_none);
        VM_ASSERT(cr->wait.wakeup_status == wakeup_none);
        cr->wait.status = wait_yielding;
        if (exc) {
            cr->wait.yielded_basket.type = basket_type_exception; // TODO opt
        }

        // leave in waiting status
    }
    ractor_unlock(cr);
}

void
rb_ractor_atexit(rb_execution_context_t *ec, VALUE result)
{
    rb_ractor_t *cr = rb_ec_ractor_ptr(ec);
    ractor_atexit_yield(ec, cr, result, false);
    ractor_close_incoming(ec, cr);
    ractor_close_outgoing(ec, cr);
}

void
rb_ractor_atexit_exception(rb_execution_context_t *ec)
{
    rb_ractor_t *cr = rb_ec_ractor_ptr(ec);
    ractor_atexit_yield(ec, cr, ec->errinfo, true);
    ractor_close_incoming(ec, cr);
    ractor_close_outgoing(ec, cr);
}

void
rb_ractor_recv_parameters(rb_execution_context_t *ec, rb_ractor_t *r, int len, VALUE *ptr)
{
    for (int i=0; i<len; i++) {
        ptr[i] = ractor_recv(ec, r);
    }
}

void
rb_ractor_send_parameters(rb_execution_context_t *ec, rb_ractor_t *r, VALUE args)
{
    int len = RARRAY_LENINT(args);
    for (int i=0; i<len; i++) {
        ractor_send(ec, r, RARRAY_AREF(args, i), false);
    }
}

#include "ractor.rbinc"

static VALUE
ractor_moved_missing(int argc, VALUE *argv, VALUE self)
{
    rb_raise(rb_eRactorMovedError, "can not send any methods to a moved object");
}

void
Init_Ractor(void)
{
    rb_cRactor = rb_define_class("Ractor", rb_cObject);
    rb_eRactorError       = rb_define_class_under(rb_cRactor, "Error", rb_eRuntimeError);
    rb_eRactorRemoteError = rb_define_class_under(rb_cRactor, "RemoteError", rb_eRactorError);
    rb_eRactorMovedError  = rb_define_class_under(rb_cRactor, "MovedError",  rb_eRactorError);
    rb_eRactorClosedError = rb_define_class_under(rb_cRactor, "ClosedError", rb_eStopIteration);

    rb_cRactorMovedObject = rb_define_class_under(rb_cRactor, "MovedObject", rb_cBasicObject);
    rb_undef_alloc_func(rb_cRactorMovedObject);
    rb_define_method(rb_cRactorMovedObject, "method_missing", ractor_moved_missing, -1);

    // override methods defined in BasicObject
    rb_define_method(rb_cRactorMovedObject, "__send__", ractor_moved_missing, -1);
    rb_define_method(rb_cRactorMovedObject, "!", ractor_moved_missing, -1);
    rb_define_method(rb_cRactorMovedObject, "==", ractor_moved_missing, -1);
    rb_define_method(rb_cRactorMovedObject, "!=", ractor_moved_missing, -1);
    rb_define_method(rb_cRactorMovedObject, "__id__", ractor_moved_missing, -1);
    rb_define_method(rb_cRactorMovedObject, "equal?", ractor_moved_missing, -1);
    rb_define_method(rb_cRactorMovedObject, "instance_eval", ractor_moved_missing, -1);
    rb_define_method(rb_cRactorMovedObject, "instance_exec", ractor_moved_missing, -1);

    rb_obj_freeze(rb_cRactorMovedObject);
}

MJIT_FUNC_EXPORTED bool
rb_ractor_shareable_p_continue(VALUE obj)
{
    switch (BUILTIN_TYPE(obj)) {
      case T_CLASS:
      case T_MODULE:
      case T_ICLASS:
        goto shareable;

      case T_FLOAT:
      case T_COMPLEX:
      case T_RATIONAL:
      case T_BIGNUM:
      case T_SYMBOL:
        VM_ASSERT(RB_OBJ_FROZEN_RAW(obj));
        goto shareable;

      case T_STRING:
      case T_REGEXP:
        if (RB_OBJ_FROZEN_RAW(obj) &&
            !FL_TEST_RAW(obj, RUBY_FL_EXIVAR)) {
            goto shareable;
        }
        return false;

      default:
        return false;
    }
  shareable:
    FL_SET_RAW(obj, RUBY_FL_SHAREABLE);
    return true;
}
