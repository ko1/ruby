// Ractor implementation

#include "ruby/ruby.h"
#include "ruby/thread.h"
#include "ruby/thread_native.h"
#include "vm_core.h"
#include "vm_sync.h"
#include "ractor.h"

static VALUE rb_cRactor;
static VALUE rb_eRactorError;
static VALUE rb_eRactorRemoteError;
static VALUE rb_eRactorMovedError;
static VALUE rb_eRactorClosedError;
static VALUE rb_cRactorMovedObject;

bool ruby_multi_ractor;
void vm_ractor_blocking_cnt_inc(rb_vm_t *vm, rb_ractor_t *r, const char *file, int line);


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
ractor_lock(rb_ractor_t *r, const char *file, int line)
{
    RUBY_DEBUG_LOG2(file, line, "r:%u%s", r->id, GET_RACTOR() == r ? " (self)" : "");

    ASSERT_ractor_unlocking(r);
    rb_native_mutex_lock(&r->lock);

#if RACTOR_CHECK_MODE > 0
    r->locked_by = GET_RACTOR()->self;
#endif
}

static void
ractor_lock_self(rb_ractor_t *cr, const char *file, int line)
{
    RUBY_DEBUG_LOG2(file, line, "", 0);

    VM_ASSERT(cr == GET_RACTOR());
    VM_ASSERT(cr->locked_by != cr->self);
    rb_native_mutex_lock(&cr->lock);
#if RACTOR_CHECK_MODE > 0
    cr->locked_by = cr->self;
#endif
}

static void
ractor_unlock(rb_ractor_t *r, const char *file, int line)
{
    ASSERT_ractor_locking(r);
#if RACTOR_CHECK_MODE > 0
    r->locked_by = Qnil;
#endif
    rb_native_mutex_unlock(&r->lock);

    RUBY_DEBUG_LOG2(file, line, "r:%u%s", r->id, GET_RACTOR() == r ? " (self)" : "");
}

static void
ractor_unlock_self(rb_ractor_t *cr)
{
    VM_ASSERT(cr == GET_RACTOR());
    VM_ASSERT(cr->locked_by == cr->self);
#if RACTOR_CHECK_MODE > 0
    cr->locked_by = Qnil;
#endif
    rb_native_mutex_unlock(&cr->lock);

    RUBY_DEBUG_LOG("", 0);
}

#define RACTOR_LOCK(r) ractor_lock(r, __FILE__, __LINE__)
#define RACTOR_LOCK_SELF(r) ractor_lock_self(r, __FILE__, __LINE__)
#define RACTOR_UNLOCK(r) ractor_unlock(r, __FILE__, __LINE__)

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

static const char *
ractor_status_str(enum ractor_status status)
{
    switch (status) {
      case ractor_created: return "created";
      case ractor_running: return "running";
      case ractor_blocking: return "blocking";
      case ractor_terminated: return "terminated";
    }
    rb_bug("unreachable");
}

static void
ractor_status_set(rb_ractor_t *r, enum ractor_status status)
{
    RUBY_DEBUG_LOG("r:%u [%s]->[%s]", r->id, ractor_status_str(r->status_), ractor_status_str(status));

    // check 1
    if (r->status_ != ractor_created) {
        VM_ASSERT(r == GET_RACTOR()); // only self-modification is allowed.
        ASSERT_vm_locking();
    }

    // check2: transition check. assume it will be vanished on non-debug build.
    switch (r->status_) {
      case ractor_created:
        VM_ASSERT(status == ractor_blocking);
        break;
      case ractor_running:
        VM_ASSERT(status == ractor_blocking||
                  status == ractor_terminated);
        break;
      case ractor_blocking:
        VM_ASSERT(status == ractor_running);
        break;
      case ractor_terminated:
        VM_ASSERT(0); // unreachable
        break;
    }

    r->status_ = status;
}

static bool
ractor_status_p(rb_ractor_t *r, enum ractor_status status)
{
    return rb_ractor_status_p(r, status);
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
    rb_gc_mark(r->loc);
    rb_gc_mark(r->name);

    rb_thread_t *th;
    list_for_each(&r->threads.set, th, lt_node) {
        VM_ASSERT(th != NULL);
        rb_gc_mark(th->self);
    }
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

    RACTOR_LOCK(r);
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
    RACTOR_UNLOCK(r);

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
    RACTOR_LOCK_SELF(cr);
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

    RACTOR_LOCK(r);
    if (r->wait.wakeup_status == wakeup_none) {
        r->wait.wakeup_status = wakeup_by_interrupt;
        rb_native_cond_signal(&r->wait.cond);
    }
    RACTOR_UNLOCK(r);
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
      case wakeup_by_retry: return "by_retry";
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

    RACTOR_UNLOCK(cr);
    rb_nogvl(ractor_sleep_wo_gvl, cr,
             ractor_sleep_interrupt, cr,
             RB_NOGVL_UBF_ASYNC_SAFE);
    RACTOR_LOCK(cr);
}

static bool
ractor_sleeping_by(const rb_ractor_t *r, enum ractor_wait_status wait_status)
{
    return (r->wait.status & wait_status) && r->wait.wakeup_status == wakeup_none;
}

static bool
ractor_wakeup(rb_ractor_t *r, enum ractor_wait_status wait_status, enum ractor_wakeup_status wakeup_status)
{
    ASSERT_ractor_locking(r);

    // fprintf(stderr, "%s r:%p status:%s/%s wakeup_status:%s/%s\n", __func__, r,
    //         wait_status_str(r->wait.status), wait_status_str(wait_status),
    //         wakeup_status_str(r->wait.wakeup_status), wakeup_status_str(wakeup_status));

    if (ractor_sleeping_by(r, wait_status)) {
        r->wait.wakeup_status = wakeup_status;
        rb_native_cond_signal(&r->wait.cond);
        return true;
    }
    else {
        return false;
    }
}

static void
ractor_register_taking(rb_ractor_t *r, rb_ractor_t *cr)
{
    VM_ASSERT(cr == GET_RACTOR());
    bool retry_try = false;

    RACTOR_LOCK(r);
    {
        if (ractor_sleeping_by(r, wait_yielding)) {
            // already waiting for yielding. retry try_take.
            retry_try = true;
        }
        else {
            // insert cr into taking list
            struct rb_ractor_waiting_list *wl = &r->taking_ractors;

            for (int i=0; i<wl->cnt; i++) {
                if (wl->ractors[i] == cr) {
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
            wl->ractors[wl->cnt++] = cr;
        }
    }
    RACTOR_UNLOCK(r);

    if (retry_try) {
        RACTOR_LOCK(cr);
        {
            if (cr->wait.wakeup_status == wakeup_none) {
                VM_ASSERT(cr->wait.status != wait_none);

                cr->wait.wakeup_status = wakeup_by_retry;
                cr->wait.status = wait_none;
            }
        }
        RACTOR_UNLOCK(cr);
    }
}

static void
ractor_waiting_list_del(rb_ractor_t *r, struct rb_ractor_waiting_list *wl, rb_ractor_t *wr)
{
    RACTOR_LOCK(r);
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
    RACTOR_UNLOCK(r);
}

static rb_ractor_t *
ractor_waiting_list_shift(rb_ractor_t *r, struct rb_ractor_waiting_list *wl)
{
    ASSERT_ractor_locking(r);
    VM_ASSERT(&r->taking_ractors == wl);

    if (wl->cnt > 0) {
        rb_ractor_t *tr = wl->ractors[0];
        for (int i=1; i<wl->cnt; i++) {
            wl->ractors[i-1] = wl->ractors[i];
        }
        wl->cnt--;
        return tr;
    }
    else {
        return NULL;
    }
}

static VALUE
ractor_recv(rb_execution_context_t *ec, rb_ractor_t *r)
{
    VM_ASSERT(r == rb_ec_ractor_ptr(ec));
    VALUE v;

    while ((v = ractor_try_recv(ec, r)) == Qundef) {
        RACTOR_LOCK(r);
        {
            if (ractor_queue_empty_p(r, &r->incoming_queue)) {
                VM_ASSERT(r->wait.status == wait_none);
                VM_ASSERT(r->wait.wakeup_status == wakeup_none);
                r->wait.status = wait_recving;

                ractor_sleep(ec, r);

                r->wait.wakeup_status = wakeup_none;
            }
        }
        RACTOR_UNLOCK(r);
    }

    return v;
}

static void
ractor_send_basket(rb_execution_context_t *ec, rb_ractor_t *r, struct rb_ractor_basket *b)
{
    bool closed = false;
    struct rb_ractor_queue *rq = &r->incoming_queue;

    RACTOR_LOCK(r);
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
    RACTOR_UNLOCK(r);

    if (closed) {
        rb_raise(rb_eRactorClosedError, "The incoming-port is already closed");
    }
}

static void
ractor_basket_setup(rb_execution_context_t *ec, struct rb_ractor_basket *basket, VALUE obj, VALUE move, bool exc)
{
    basket->sender = rb_ec_ractor_ptr(ec)->self;

    if (!RTEST(move)) {
        ractor_copy_setup(basket, obj);
    }
    else {
        ractor_move_setup(basket, obj);
    }

    if (exc) {
        basket->type = basket_type_exception;
    }
}

static VALUE
ractor_send(rb_execution_context_t *ec, rb_ractor_t *r, VALUE obj, VALUE move)
{
    struct rb_ractor_basket basket;
    ractor_basket_setup(ec, &basket, obj, move, false);
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

    RACTOR_LOCK(r);
    {
        if (ractor_wakeup(r, wait_yielding, wakeup_by_take)) {
            VM_ASSERT(r->wait.yielded_basket.type != basket_type_none);
            basket = r->wait.yielded_basket;
            ractor_basket_clear(&r->wait.yielded_basket);
        }
        else if (r->outgoing_port_closed) {
            bp();
            closed = true;
        }
        else {
            // not reached.
        }
    }
    RACTOR_UNLOCK(r);

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
ractor_try_yield(rb_execution_context_t *ec, rb_ractor_t *cr, struct rb_ractor_basket *basket)
{
    ASSERT_ractor_unlocking(cr);
    VM_ASSERT(basket->type != basket_type_none);

    rb_ractor_t *r;

  retry_shift:
    RACTOR_LOCK(cr);
    {
        r = ractor_waiting_list_shift(cr, &cr->taking_ractors);
    }
    RACTOR_UNLOCK(cr);

    if (r) {
        bool retry_shift = false;

        RACTOR_LOCK(r);
        {
            if (ractor_wakeup(r, wait_taking, wakeup_by_yield)) {
                VM_ASSERT(r->wait.taken_basket.type == basket_type_none);
                r->wait.taken_basket = *basket;
            }
            else {
                retry_shift = true;
            }
        }
        RACTOR_UNLOCK(r);

        if (retry_shift) {
            // get candidate take-waiting ractor, but already woke up by another reason.
            // retry to check another ractor.
            goto retry_shift;
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
    VALUE ret = Qundef;
    int i;
    enum ractor_wait_status wait_status = 0;
    struct rb_ractor_basket yielding_basket = {
        .type = basket_type_none,
    };

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
        actions[i].v = Qundef;
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
                {
                    bool yield_result;

                    if (yielding_basket.type == basket_type_none) {
                        ractor_basket_setup(ec, &yielding_basket, yielded_value, move, false);
                    }
                    yield_result = ractor_try_yield(ec, cr, &yielding_basket);

                    if (yield_result) {
                        *ret_r = ID2SYM(rb_intern("yield"));
                        ret = Qnil;
                        goto cleanup;
                    }
                }
                break;
            }
        }

        // prepare yielding
        if (yield_p) {
            VM_ASSERT(yielding_basket.type != basket_type_none);
            if (cr->wait.yielded_basket.type == basket_type_none) {
                cr->wait.yielded_basket = yielding_basket;
            }
        }

        // setup waiting status
        RACTOR_LOCK(cr);
        {
            VM_ASSERT(cr->wait.status == wait_none);
            VM_ASSERT(cr->wait.wakeup_status == wakeup_none);
            cr->wait.status = wait_status;
        }
        RACTOR_UNLOCK(cr);

        // prepare waiting
        for (i=0; i<alen; i++) {
            rb_ractor_t *r;
            switch (actions[i].type) {
              case ractor_select_action_take:
                r = RACTOR_PTR(actions[i].v);
                ractor_register_taking(r, cr);
                break;
              case ractor_select_action_yield:
              case ractor_select_action_recv:
                break;
            }
        }

        // wait
        RACTOR_LOCK(cr);
        {
            if (cr->wait.wakeup_status == wakeup_none) {
                VM_ASSERT(cr->wait.status != wait_none);
                ractor_sleep(ec, cr);
                RUBY_DEBUG_LOG("awaken %s", wakeup_status_str(cr->wait.wakeup_status));
            }
            else {
                RUBY_DEBUG_LOG("no need to sleep %s->%s",
                               wait_status_str(cr->wait.status),
                               wakeup_status_str(cr->wait.wakeup_status));
                cr->wait.status = wait_none;
            }
        }
        RACTOR_UNLOCK(cr);

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
          case wakeup_by_retry:
            // Retry request.
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
            ret = Qundef;
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

    VM_ASSERT(ret != Qundef);
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

    RACTOR_LOCK(r);
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
    RACTOR_UNLOCK(r);
    return prev;
}

static VALUE
ractor_close_outgoing(rb_execution_context_t *ec, rb_ractor_t *cr)
{
    VALUE prev;

    RACTOR_LOCK(cr);
    {
        if (!cr->outgoing_port_closed) {
            prev = Qfalse;
            cr->outgoing_port_closed = true;
        }
        else {
            prev = Qtrue;
        }

        // wakeup all taking ractors
        rb_ractor_t *taking_ractor;
        while ((taking_ractor = ractor_waiting_list_shift(cr, &cr->taking_ractors)) != NULL) {
            RACTOR_LOCK(taking_ractor);
            ractor_wakeup(taking_ractor, wait_taking, wakeup_by_close);
            RACTOR_UNLOCK(taking_ractor);
        }
    }
    RACTOR_UNLOCK(cr);
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
vm_insert_ractor0(rb_vm_t *vm, rb_ractor_t *r)
{
    RUBY_DEBUG_LOG("r:%u ractor.cnt:%u++", r->id, vm->ractor.cnt);
    VM_ASSERT(!rb_multi_ractor_p() || RB_VM_LOCKED_P());

    list_add_tail(&vm->ractor.set, &r->vmlr_node);
    vm->ractor.cnt++;
}

static void
vm_insert_ractor(rb_vm_t *vm, rb_ractor_t *r)
{
    VM_ASSERT(ractor_status_p(r, ractor_created));

    if (rb_multi_ractor_p()) {
        RB_VM_LOCK();
        {
            vm_insert_ractor0(vm, r);
            vm_ractor_blocking_cnt_inc(vm, r, __FILE__, __LINE__);
        }
        RB_VM_UNLOCK();
    }
    else {
        vm_insert_ractor0(vm, r);

        if (vm->ractor.cnt == 1) {
            // main ractor
            ractor_status_set(r, ractor_blocking);
            ractor_status_set(r, ractor_running);
        }
        else {
            vm_ractor_blocking_cnt_inc(vm, r, __FILE__, __LINE__);

            // enable multi-ractor mode
            ruby_multi_ractor = true;
        }
    }
}

static void
vm_remove_ractor(rb_vm_t *vm, rb_ractor_t *r)
{
    VM_ASSERT(ractor_status_p(r, ractor_running));
    VM_ASSERT(vm->ractor.cnt > 1);

    RB_VM_LOCK();
    {
        RUBY_DEBUG_LOG("ractor.cnt:%u-- terminate_waiting:%d",
                       vm->ractor.cnt,  vm->ractor.sync.terminate_waiting);

        VM_ASSERT(vm->ractor.cnt > 0);
        list_del(&r->vmlr_node);

        if (vm->ractor.cnt <= 2 && vm->ractor.sync.terminate_waiting) {
            rb_native_cond_signal(&vm->ractor.sync.terminate_cond);
        }
        vm->ractor.cnt--;

        ractor_status_set(r, ractor_terminated);
    }
    RB_VM_UNLOCK();
}

static VALUE
ractor_alloc(VALUE klass, VALUE name, VALUE loc)
{
    rb_ractor_t *r;
    VALUE rv = TypedData_Make_Struct(klass, rb_ractor_t, &ractor_data_type, r);
    FL_SET_RAW(rv, RUBY_FL_SHAREABLE);

    // naming
    r->id = ractor_next_id();
    r->name = name;
    r->loc = loc;
    r->self = rv;
    VM_ASSERT(ractor_status_p(r, ractor_created));
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

void rb_gvl_init(rb_global_vm_lock_t *gvl);

void
rb_ractor_living_threads_init(rb_ractor_t *r)
{
    list_head_init(&r->threads.set);
    r->threads.cnt = 0;
    r->threads.blocking_cnt = 0;
}

static void
ractor_init(rb_ractor_t *r)
{
    ractor_queue_setup(&r->incoming_queue);
    rb_native_mutex_initialize(&r->lock);
    rb_native_cond_initialize(&r->wait.cond);
    rb_native_cond_initialize(&r->barrier_wait_cond);

    // thread management
    rb_gvl_init(&r->threads.gvl);
    rb_ractor_living_threads_init(r);
}

void
rb_ractor_main_setup(rb_vm_t *vm, rb_ractor_t *r, rb_thread_t *th)
{
    r->self = TypedData_Wrap_Struct(rb_cRactor, &ractor_data_type, r);
    FL_SET_RAW(r->self, RUBY_FL_SHAREABLE);
    ractor_init(r);
    r->threads.main = th;

    rb_ractor_living_threads_insert(r, th);
}

static VALUE
ractor_create(rb_execution_context_t *ec, VALUE self, VALUE loc, VALUE name, VALUE args, VALUE block)
{
    VALUE rv = ractor_alloc(self, name, loc);
    rb_ractor_t *r = RACTOR_PTR(rv);
    ractor_init(r);

    RUBY_DEBUG_LOG("r:%u", r->id);

    rb_thread_create_ractor(r, args, block);
    return rv;
}

static void
ractor_atexit_yield(rb_execution_context_t *ec, rb_ractor_t *cr, VALUE v, bool exc)
{
    ASSERT_ractor_unlocking(cr);

    struct rb_ractor_basket basket;
    ractor_basket_setup(ec, &basket, v, Qfalse, exc);
 
  retry:
    if (ractor_try_yield(ec, cr, &basket)) {
        // OK.
    }
    else {
        bool retry = false;
        RACTOR_LOCK(cr);
        {
            if (cr->taking_ractors.cnt == 0) {
                cr->wait.yielded_basket = basket;

                VM_ASSERT(cr->wait.status == wait_none);
                cr->wait.status = wait_yielding;
            }
            else {
                retry = true; // another ractor is waiting for the yield.
            }
        }
        RACTOR_UNLOCK(cr);

        if (retry) goto retry;
    }
}

void
rb_ractor_teardown(rb_execution_context_t *ec)
{
    rb_ractor_t *cr = rb_ec_ractor_ptr(ec);
    ractor_close_incoming(ec, cr);
    ractor_close_outgoing(ec, cr);
}

void
rb_ractor_atexit(rb_execution_context_t *ec, VALUE result)
{
    rb_ractor_t *cr = rb_ec_ractor_ptr(ec);
    ractor_atexit_yield(ec, cr, result, false);
}

void
rb_ractor_atexit_exception(rb_execution_context_t *ec)
{
    rb_ractor_t *cr = rb_ec_ractor_ptr(ec);
    ractor_atexit_yield(ec, cr, ec->errinfo, true);
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

VALUE
rb_ractor_self(const rb_ractor_t *r)
{
    return r->self;
}

MJIT_FUNC_EXPORTED int
rb_ractor_main_p(void)
{
    rb_execution_context_t *ec = GET_EC();
    return rb_ec_ractor_ptr(ec) == rb_ec_vm_ptr(ec)->ractor.main_ractor;
}

rb_global_vm_lock_t *
rb_ractor_gvl(rb_ractor_t *r)
{
    return &r->threads.gvl;
}

int
rb_ractor_living_thread_num(const rb_ractor_t *r)
{
    return r->threads.cnt;
}

VALUE
rb_ractor_thread_list(rb_ractor_t *r)
{
    VALUE ary = rb_ary_new();
    rb_thread_t *th = 0;

    RACTOR_LOCK(r);
    list_for_each(&r->threads.set, th, lt_node) {
        switch (th->status) {
          case THREAD_RUNNABLE:
          case THREAD_STOPPED:
          case THREAD_STOPPED_FOREVER:
            rb_ary_push(ary, th->self);
          default:
            break;
        }
    }
    RACTOR_UNLOCK(r);
    return ary;
}

void
rb_ractor_living_threads_insert(rb_ractor_t *r, rb_thread_t *th)
{
    VM_ASSERT(th != NULL);

    RACTOR_LOCK(r);
    {
        RUBY_DEBUG_LOG("r(%d)->threads.cnt:%d++", r->id, r->threads.cnt);
        list_add_tail(&r->threads.set, &th->lt_node);
        r->threads.cnt++;
    }
    RACTOR_UNLOCK(r);

    // first thread for a ractor
    if (r->threads.cnt == 1) {
        VM_ASSERT(ractor_status_p(r, ractor_created));
        vm_insert_ractor(th->vm, r);
        r->threads.running = th;
    }
}

void
vm_ractor_blocking_cnt_inc(rb_vm_t *vm, rb_ractor_t *r, const char *file, int line)
{
    ractor_status_set(r, ractor_blocking);

    RUBY_DEBUG_LOG2(file, line, "vm->ractor.blocking_cnt:%d++", vm->ractor.blocking_cnt);
    vm->ractor.blocking_cnt++;
    VM_ASSERT(vm->ractor.blocking_cnt <= vm->ractor.cnt);
}

void
rb_vm_ractor_blocking_cnt_inc(rb_vm_t *vm, rb_ractor_t *cr, const char *file, int line)
{
    ASSERT_vm_locking();
    VM_ASSERT(GET_RACTOR() == cr);
    vm_ractor_blocking_cnt_inc(vm, cr, file, line);
}

void
rb_vm_ractor_blocking_cnt_dec(rb_vm_t *vm, rb_ractor_t *cr, const char *file, int line)
{
    ASSERT_vm_locking();
    VM_ASSERT(GET_RACTOR() == cr);

    RUBY_DEBUG_LOG2(file, line, "vm->ractor.blocking_cnt:%d--", vm->ractor.blocking_cnt);
    VM_ASSERT(vm->ractor.blocking_cnt > 0);
    vm->ractor.blocking_cnt--;

    ractor_status_set(cr, ractor_running);
}

static void
ractor_check_blocking(rb_ractor_t *cr, unsigned int remaind_thread_cnt, const char *file, int line)
{
    VM_ASSERT(cr == GET_RACTOR());

    RUBY_DEBUG_LOG2(file, line,
                    "cr->threads.cnt:%u cr->threads.blocking_cnt:%u vm->ractor.cnt:%u vm->ractor.blocking_cnt:%u",
                    cr->threads.cnt, cr->threads.blocking_cnt,
                    GET_VM()->ractor.cnt, GET_VM()->ractor.blocking_cnt);

    VM_ASSERT(cr->threads.cnt >= cr->threads.blocking_cnt + 1);

    if (remaind_thread_cnt > 0 &&
        // will be block
        cr->threads.cnt == cr->threads.blocking_cnt + 1) {
        // change ractor status: running -> blocking
        rb_vm_t *vm = GET_VM();
        ASSERT_vm_unlocking();

        RB_VM_LOCK();
        {
            rb_vm_ractor_blocking_cnt_inc(vm, cr, file, line);
        }
        RB_VM_UNLOCK();
    }
}

void
rb_ractor_living_threads_remove(rb_ractor_t *cr, rb_thread_t *th)
{
    VM_ASSERT(cr == GET_RACTOR());
    RUBY_DEBUG_LOG("r->threads.cnt:%d--", cr->threads.cnt);
    ractor_check_blocking(cr, cr->threads.cnt - 1, __FILE__, __LINE__);

    RACTOR_LOCK(cr);
    {
        list_del(&th->lt_node);
        cr->threads.cnt--;
    }
    RACTOR_UNLOCK(cr);

    if (cr->threads.cnt == 0) {
        vm_remove_ractor(th->vm, cr);
    }
}

void
rb_ractor_blocking_threads_inc(rb_ractor_t *cr, const char *file, int line)
{
    RUBY_DEBUG_LOG2(file, line, "cr->threads.blocking_cnt:%d++", cr->threads.blocking_cnt);

    VM_ASSERT(cr->threads.cnt > 0);
    VM_ASSERT(cr == GET_RACTOR());

    ractor_check_blocking(cr, cr->threads.cnt, __FILE__, __LINE__);
    cr->threads.blocking_cnt++;
}

void
rb_ractor_blocking_threads_dec(rb_ractor_t *cr, const char *file, int line)
{
    RUBY_DEBUG_LOG2(file, line,
                    "r->threads.blocking_cnt:%d--, r->threads.cnt:%u",
                    cr->threads.blocking_cnt, cr->threads.cnt);

    VM_ASSERT(cr == GET_RACTOR());

    if (cr->threads.cnt == cr->threads.blocking_cnt) {
        rb_vm_t *vm = GET_VM();

        RB_VM_LOCK_ENTER();
        {
            rb_vm_ractor_blocking_cnt_dec(vm, cr, __FILE__, __LINE__);
        }
        RB_VM_LOCK_LEAVE();
    }

    cr->threads.blocking_cnt--;
}

void
rb_ractor_vm_barrier_interrupt_running_thread(rb_ractor_t *r)
{
    VM_ASSERT(r != GET_RACTOR());
    ASSERT_ractor_unlocking(r);
    ASSERT_vm_locking();

    RACTOR_LOCK(r);
    {
        if (ractor_status_p(r, ractor_running)) {
            rb_thread_t *th = r->threads.running;
            VM_ASSERT(th != NULL);
            RUBY_VM_SET_VM_BARRIER_INTERRUPT(th->ec);
        }
    }
    RACTOR_UNLOCK(r);
}

void
rb_ractor_terminate_interrupt_main_thread(rb_ractor_t *r)
{
    VM_ASSERT(r != GET_RACTOR());
    ASSERT_ractor_unlocking(r);
    ASSERT_vm_locking();

    rb_thread_t *main_th = r->threads.main;

    if (main_th) {
        if (main_th->status != THREAD_KILLED) {
            RUBY_VM_SET_TERMINATE_INTERRUPT(main_th->ec);
            rb_threadptr_interrupt(main_th);
        }
        else {
            RUBY_DEBUG_LOG("killed (%p)", main_th);
        }
    }
}

void rb_thread_terminate_all(void); // thread.c

void
ractor_terminal_interrupt_all(rb_vm_t *vm)
{
    if (vm->ractor.cnt > 1) {
        // send terminate notification to all ractors
        rb_ractor_t *r;
        list_for_each(&vm->ractor.set, r, vmlr_node) {
            if (r != vm->ractor.main_ractor) {
                rb_ractor_terminate_interrupt_main_thread(r);
            }
        }
    }
}

void
rb_ractor_terminate_all(void)
{
    rb_vm_t *vm = GET_VM();
    rb_ractor_t *cr = vm->ractor.main_ractor;

    VM_ASSERT(cr == GET_RACTOR()); // only main-ractor's main-thread should kick it.

    if (vm->ractor.cnt > 1) {
        RB_VM_LOCK();
        ractor_terminal_interrupt_all(vm); // kill all ractors
        RB_VM_UNLOCK();
    }
    rb_thread_terminate_all(); // kill other threads in main-ractor and wait

    RB_VM_LOCK();
    {
        while (vm->ractor.cnt > 1) {
            RUBY_DEBUG_LOG("terminate_waiting:%d", vm->ractor.sync.terminate_waiting);
            vm->ractor.sync.terminate_waiting = true;

            // wait for 1sec
            rb_vm_ractor_blocking_cnt_inc(vm, cr, __FILE__, __LINE__);
            rb_vm_cond_timedwait(vm, &vm->ractor.sync.terminate_cond, 1000 /* ms */);
            rb_vm_ractor_blocking_cnt_dec(vm, cr, __FILE__, __LINE__);

            ractor_terminal_interrupt_all(vm);
        }
    }
    RB_VM_UNLOCK();
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

void
rb_ractor_dump(void)
{
    rb_vm_t *vm = GET_VM();
    rb_ractor_t *r;

    list_for_each(&vm->ractor.set, r, vmlr_node) {
        if (r != vm->ractor.main_ractor) {
            fprintf(stderr, "r:%u (%s)\n", r->id, ractor_status_str(r->status_));
        }
    }
}
