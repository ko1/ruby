// Ractor implementation

#include "ruby/ruby.h"
#include "ruby/thread.h"
#include "ruby/thread_native.h"
#include "vm_core.h"
#include "ractor.h"

static VALUE rb_cRactor;
static VALUE rb_eRactorRemoteError;

static VALUE rb_cRactorChannel;
static VALUE rb_eRactorChannelClosedError;
static VALUE rb_eRactorChannelError;
static VALUE rb_cRactorMovedObject;

typedef struct rb_ractor_struct {
    // default channels
    VALUE incoming_channel;
    VALUE outgoing_channel;

    // sleep management
    rb_nativethread_lock_t sleep_lock;
    rb_nativethread_cond_t sleep_cond;
    bool sleep_interrupted;

    VALUE running_thread;

    // misc
    VALUE self;

    // identity
    uint32_t id;
    VALUE name;
    VALUE loc;
} rb_ractor_t;

enum rb_ractor_channel_basket_type {
    basket_type_shareable,
    basket_type_copy_marshal,
    basket_type_copy_custom,
    basket_type_move,
    basket_type_exception,
};

struct rb_ractor_channel_basket {
    int type;
    VALUE v;
    VALUE sender;
};

typedef struct rb_ractor_channel_struct {
    struct rb_ractor_channel_basket *baskets;
    int cnt;
    int size;

    rb_nativethread_lock_t lock;

    struct channel_waitings {
        int cnt;
        int size;
        rb_ractor_t **ractors;
    } waiting;

    bool closed;
} rb_ractor_channel_t;

static void
ractor_mark(void *ptr)
{
    // fprintf(stderr, "%s:%p\n", __func__, ptr);
    rb_ractor_t *g = (rb_ractor_t *)ptr;

    rb_gc_mark(g->incoming_channel);
    rb_gc_mark(g->outgoing_channel);
    rb_gc_mark(g->running_thread);
    rb_gc_mark(g->loc);
    rb_gc_mark(g->name);
}

static void
ractor_free(void *ptr)
{
    // fprintf(stderr, "%s:%p\n", __func__, ptr);

    rb_ractor_t *g = (rb_ractor_t *)ptr;
    rb_native_mutex_destroy(&g->sleep_lock);
    rb_native_cond_destroy(&g->sleep_cond);
}

static size_t
ractor_memsize(const void *ptr)
{
    return sizeof(rb_ractor_t);
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

static void
ractor_channel_mark(void *ptr)
{
    // fprintf(stderr, "%s:%p\n", __func__, ptr);
    rb_ractor_channel_t *gc = (rb_ractor_channel_t *)ptr;
    for (int i=0; i<gc->cnt; i++) {
        rb_gc_mark(gc->baskets[i].v);
        rb_gc_mark(gc->baskets[i].sender);
    }
}

static void
ractor_channel_free(void *ptr)
{
    // fprintf(stderr, "%s:%p\n", __func__, ptr);
    rb_ractor_channel_t *gc = (rb_ractor_channel_t *)ptr;
    ruby_xfree(gc->waiting.ractors);
    rb_native_mutex_destroy(&gc->lock);
}

static size_t
ractor_channel_memsize(const void *ptr)
{
    // TODO
    return sizeof(rb_ractor_channel_t);
}

static const rb_data_type_t ractor_channel_data_type = {
    "ractor/channel",
    {
        ractor_channel_mark,
	ractor_channel_free,
        ractor_channel_memsize,
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

bool
rb_ractor_channel_p(VALUE gcv)
{
    if (rb_typeddata_is_kind_of(gcv, &ractor_channel_data_type)) {
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
    rb_ractor_t *g = DATA_PTR(self);
    // TODO: check
    return g;
}

static inline rb_ractor_channel_t *
RACTOR_CHANNEL_PTR(VALUE self)
{
    VM_ASSERT(rb_ractor_channel_p(self));
    rb_ractor_channel_t *gc = DATA_PTR(self);
    // TODO: check
    return gc;
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

static VALUE
ractor_channel_alloc(VALUE klass)
{
    rb_ractor_channel_t *gc;
    VALUE gcv = TypedData_Make_Struct(klass, rb_ractor_channel_t, &ractor_channel_data_type, gc);
    FL_SET_RAW(gcv, RUBY_FL_SHAREABLE);

    gc->size = 2;
    gc->cnt = 0;
    gc->baskets = ALLOC_N(struct rb_ractor_channel_basket, gc->size);

    gc->waiting.cnt = 0;
    gc->waiting.size = 0;
    gc->waiting.ractors = NULL;

    rb_native_mutex_initialize(&gc->lock);
    return gcv;
}

static VALUE
ractor_channel_create(rb_execution_context_t *ec)
{
    VALUE gcv = ractor_channel_alloc(rb_cRactorChannel);
    // rb_ractor_channel_t *gc = RACTOR_CHANNEL_PTR(gcv);
    return gcv;
}

VALUE rb_newobj_with(VALUE src);

static VALUE
ractor_channel_move_new(VALUE obj)
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
ractor_channel_move_shallow_copy(VALUE obj)
{
    if (rb_ractor_shareable_p(obj)) {
        return obj;
    }
    else {
        switch (BUILTIN_TYPE(obj)) {
          case T_STRING:
          case T_FILE:
            if (!FL_TEST_RAW(obj, RUBY_FL_EXIVAR)) {
                return ractor_channel_move_new(obj);
            }
            break;
          case T_ARRAY:
            if (!FL_TEST_RAW(obj, RUBY_FL_EXIVAR)) {
                VALUE ary = ractor_channel_move_new(obj);
                long len = RARRAY_LEN(ary);
                for (long i=0; i<len; i++) {
                    VALUE e = RARRAY_AREF(ary, i);
                    RARRAY_ASET(ary, i, ractor_channel_move_shallow_copy(e)); // confirm WB
                }
                return ary;
            }
            break;
          default:
            break;
        }

        rb_raise(rb_eRactorChannelError, "can't move this this kind of object:%"PRIsVALUE, obj);
    }
}

static void
ractor_channel_move_setup(struct rb_ractor_channel_basket *b, VALUE obj)
{
    if (rb_ractor_shareable_p(obj)) {
        b->type = basket_type_shareable;
        b->v = obj;
    }
    else {
        b->type = basket_type_move;
        b->v = ractor_channel_move_shallow_copy(obj);
        return;
    }
}

static VALUE
ractor_channel_moved_setup(VALUE obj)
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
                ractor_channel_moved_setup(e);
            }
        }
        break;
      default:
        rb_bug("unreachable");
    }
#endif
    return obj;
}

static VALUE
ractor_channel_recv_accept(struct rb_ractor_channel_basket *b)
{
    switch (b->type) {
      case basket_type_shareable:
        VM_ASSERT(rb_ractor_shareable_p(b->v));
        return b->v;
      case basket_type_copy_marshal:
        return rb_marshal_load(b->v);
      case basket_type_exception:
        {
            VALUE cause = rb_marshal_load(b->v);
            VALUE err = rb_exc_new_cstr(rb_eRactorRemoteError, "thrown by remote Ractor.");
            rb_ivar_set(err, rb_intern("@ractor"), b->sender);
            rb_ec_setup_exception(NULL, err, cause);
            rb_exc_raise(err);
        }
        // unreachable
      case basket_type_move:
        return ractor_channel_moved_setup(b->v);
      default:
        rb_bug("unreachable");
    }
}

static void
ractor_channel_copy_setup(struct rb_ractor_channel_basket *b, VALUE obj)
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
        b->type = basket_type_copy_marshal;
        b->v = rb_marshal_dump(obj, Qnil);
    }
}

static VALUE
ractor_channel_try_recv(rb_execution_context_t *ec, rb_ractor_channel_t *gc)
{
    struct rb_ractor_channel_basket basket;

    rb_native_mutex_lock(&gc->lock);
    if (gc->cnt > 0) {
        basket = gc->baskets[0];

        // TODO: use Queue
        gc->cnt--;
        for (int i=0; i<gc->cnt; i++) {
            gc->baskets[i] = gc->baskets[i+1];
        }
    }
    else {
        basket.v = Qundef;
    }
    rb_native_mutex_unlock(&gc->lock);

    if (basket.v == Qundef) {
        if (gc->closed) {
            rb_raise(rb_eRactorChannelClosedError, "The send-edge is already closed");
        }
        else {
            return Qundef;
        }
    }

    return ractor_channel_recv_accept(&basket);
}

static void *
ractor_sleep_wo_gvl(void *ptr)
{
    rb_ractor_t *g = ptr;
    rb_native_mutex_lock(&g->sleep_lock);
    if (g->sleep_interrupted == false) {
        rb_native_cond_wait(&g->sleep_cond, &g->sleep_lock);
    }
    rb_native_mutex_unlock(&g->sleep_lock);
    return NULL;
}

static void
ractor_sleep_cancel(void *ptr)
{
    rb_ractor_t *g = ptr;
    rb_native_mutex_lock(&g->sleep_lock);
    rb_native_cond_signal(&g->sleep_cond);
    rb_native_mutex_unlock(&g->sleep_lock);
}

static void
ractor_sleep_setup(rb_execution_context_t *ec, rb_ractor_t *g)
{
    rb_native_mutex_lock(&g->sleep_lock);
    {
        g->sleep_interrupted = false;
    }
    rb_native_mutex_unlock(&g->sleep_lock);
}

static void
ractor_sleep(rb_execution_context_t *ec, rb_ractor_t *g)
{
    rb_native_mutex_lock(&g->sleep_lock);
    {
        while (g->sleep_interrupted == false) {
            rb_native_mutex_unlock(&g->sleep_lock);

            rb_thread_call_without_gvl(ractor_sleep_wo_gvl, g,
                                       ractor_sleep_cancel, g);

            rb_native_mutex_lock(&g->sleep_lock);
        }
    }
    rb_native_mutex_unlock(&g->sleep_lock);
}

static bool
ractor_channel_waiting_p(rb_ractor_channel_t *gc)
{
    // TODO: assert(gc->lock is locked)
    return gc->waiting.cnt > 0;
}

static void
ractor_channel_waiting_add(rb_ractor_channel_t *gc, rb_ractor_t *g)
{
    rb_native_mutex_lock(&gc->lock);
    {
        for (int i=0; i<gc->waiting.cnt; i++) {
            if (gc->waiting.ractors[i] == g) {
                // TODO: make it clean code.
                rb_native_mutex_unlock(&gc->lock);
                rb_raise(rb_eRuntimeError, "Already another thread of same ractor is waiting.");
            }
        }

        if (gc->waiting.size == 0) {
            gc->waiting.size = 4;
            gc->waiting.ractors = ALLOC_N(rb_ractor_t *, gc->waiting.size);
        }
        else if (gc->waiting.size <= gc->waiting.cnt + 1) {
            gc->waiting.size *= 2;
            REALLOC_N(gc->waiting.ractors, rb_ractor_t *, gc->waiting.size);
        }
        gc->waiting.ractors[gc->waiting.cnt++] = g;
        // fprintf(stderr, "cnt:%d size:%d\n", gc->waiting.cnt, gc->waiting.size);
    }
    rb_native_mutex_unlock(&gc->lock);
}

static void
ractor_channel_waiting_del(rb_ractor_channel_t *gc, rb_ractor_t *g)
{
    rb_native_mutex_lock(&gc->lock);
    {
        int pos = -1;
        for (int i=0; i<gc->waiting.cnt; i++) {
            if (gc->waiting.ractors[i] == g) {
                pos = i;
                break;
            }
        }
        if (pos >= 0) { // found
            gc->waiting.cnt--;
            for (int i=pos; i<gc->waiting.cnt; i++) {
                gc->waiting.ractors[i] = gc->waiting.ractors[i+1];
            }
        }
    }
    rb_native_mutex_unlock(&gc->lock);
}

static VALUE
ractor_channel_recv(rb_execution_context_t *ec, VALUE gcv)
{
    rb_ractor_t *g = rb_ec_ractor_ptr(ec);
    rb_ractor_channel_t *gc = RACTOR_CHANNEL_PTR(gcv);
    VALUE v;

    while ((v = ractor_channel_try_recv(ec, gc)) == Qundef) {
        ractor_sleep_setup(ec, g);
        ractor_channel_waiting_add(gc, g);
        ractor_sleep(ec, g);
        ractor_channel_waiting_del(gc, g);
    }

    RB_GC_GUARD(gcv);
    return v;
}

static bool
ractor_channel_wakeup(rb_ractor_channel_t *gc, rb_ractor_t *wg)
{
    bool result = false;

    rb_native_mutex_lock(&wg->sleep_lock);
    {
        if (wg->sleep_interrupted == false) {
            wg->sleep_interrupted = true;
            rb_native_cond_signal(&wg->sleep_cond);
        }
    }
    rb_native_mutex_unlock(&wg->sleep_lock);
    return result;
}

static void
ractor_channel_wakeup_all(rb_ractor_channel_t *gc)
{
    // TODO: assert(gc->lock is locked)
    for (int i=0; i<gc->waiting.cnt; i++) {
        rb_ractor_t *wg = gc->waiting.ractors[i];
        ractor_channel_wakeup(gc, wg);
    }
}

static void
ractor_channel_send_basket(rb_execution_context_t *ec, rb_ractor_channel_t *gc, struct rb_ractor_channel_basket *b)
{
    bool closed = false;

    rb_native_mutex_lock(&gc->lock);
    {
        if (gc->closed) {
            closed = true;
        }
        else {
            // enq
            if (gc->size <= gc->cnt) {
                gc->size *= 2;
                REALLOC_N(gc->baskets, struct rb_ractor_channel_basket, gc->size);
            }
            b->sender = rb_ec_ractor_ptr(ec)->self;
            gc->baskets[gc->cnt++] = *b;

            if (ractor_channel_waiting_p(gc)) {
                ractor_channel_wakeup_all(gc);
            }
        }
    }
    rb_native_mutex_unlock(&gc->lock);

    if (closed) {
        rb_raise(rb_eRactorChannelClosedError, "The recv-edge is already closed");
    }
}

static VALUE
ractor_channel_send_exception(rb_execution_context_t *ec, VALUE gcv, VALUE errinfo)
{
    rb_ractor_channel_t *gc = RACTOR_CHANNEL_PTR(gcv);
    struct rb_ractor_channel_basket basket;
    ractor_channel_copy_setup(&basket, errinfo);
    basket.type = basket_type_exception;

    ractor_channel_send_basket(ec, gc, &basket);

    return gcv;
}

static VALUE
ractor_channel_send(rb_execution_context_t *ec, VALUE gcv, VALUE obj)
{
    rb_ractor_channel_t *gc = RACTOR_CHANNEL_PTR(gcv);
    struct rb_ractor_channel_basket basket;
    ractor_channel_copy_setup(&basket, obj);
    ractor_channel_send_basket(ec, gc, &basket);
    return gcv;
}

static VALUE
ractor_channel_move(rb_execution_context_t *ec, VALUE gcv, VALUE obj)
{
    rb_ractor_channel_t *gc = RACTOR_CHANNEL_PTR(gcv);
    struct rb_ractor_channel_basket basket;
    ractor_channel_move_setup(&basket, obj);

    ractor_channel_send_basket(ec, gc, &basket);

    return gcv;
}

static VALUE
ractor_channel_close(rb_execution_context_t *ec, VALUE gcv)
{
    rb_ractor_channel_t *gc = RACTOR_CHANNEL_PTR(gcv);
    VALUE prev;

    rb_native_mutex_lock(&gc->lock);
    {
        if (!gc->closed) {
            prev = Qfalse;
            gc->closed = true;

            if (ractor_channel_waiting_p(gc)) {
                ractor_channel_wakeup_all(gc);
            }
        }
        else {
            prev = Qtrue;
        }
    }
    rb_native_mutex_unlock(&gc->lock);

    RB_GC_GUARD(gcv);
    return prev;
}

static VALUE
ractor_channel_new(VALUE self)
{
    return ractor_channel_alloc(self);
}

static VALUE
ractor_next_id(void)
{
    // TODO: lock
    return ++ractor_last_id;
}

static void
ractor_setup(rb_ractor_t *g)
{
    rb_execution_context_t *ec = GET_EC();
    g->incoming_channel = ractor_channel_create(ec);
    g->outgoing_channel = ractor_channel_create(ec);
    rb_native_mutex_initialize(&g->sleep_lock);
    rb_native_cond_initialize(&g->sleep_cond);
}

static VALUE
ractor_alloc(VALUE klass)
{
    rb_ractor_t *g;
    VALUE gv = TypedData_Make_Struct(klass, rb_ractor_t, &ractor_data_type, g);
    FL_SET_RAW(gv, RUBY_FL_SHAREABLE);

    // namig
    g->id = ractor_next_id();
    g->loc = Qnil;
    g->name = Qnil;
    g->self = gv;
    ractor_setup(g);
    return gv;
}

rb_ractor_t *
rb_ractor_main_alloc(void)
{
    rb_ractor_t *g = ruby_mimmalloc(sizeof(rb_ractor_t));
    if (g == NULL) {
	fprintf(stderr, "[FATAL] failed to allocate memory for main ractor\n");
        exit(EXIT_FAILURE);
    }
    MEMZERO(g, rb_ractor_t, 1);
    g->id = ++ractor_last_id;
    g->loc = Qnil;
    g->name = Qnil;

    return g;
}

void
rb_ractor_main_setup(rb_ractor_t *g)
{
    g->self = TypedData_Wrap_Struct(rb_cRactor, &ractor_data_type, g);
    ractor_setup(g);
}


VALUE
rb_ractor_self(const rb_ractor_t *g)
{
    return g->self;
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
    VALUE gv = ractor_alloc(self);
    rb_ractor_t *g = RACTOR_PTR(gv);
    g->running_thread = rb_thread_create_ractor(g, args, block);
    g->loc = loc;
    g->name = name;
    return gv;
}

void
rb_ractor_atexit(rb_execution_context_t *ec, VALUE result)
{
    rb_ractor_t *g = rb_ec_ractor_ptr(ec);
    ractor_channel_send(ec, g->outgoing_channel, result);
    ractor_channel_close(ec, g->outgoing_channel);
    ractor_channel_close(ec, g->incoming_channel);
}

void
rb_ractor_atexit_exception(rb_execution_context_t *ec)
{
    rb_ractor_t *g = rb_ec_ractor_ptr(ec);
    ractor_channel_send_exception(ec, g->outgoing_channel, ec->errinfo);
    ractor_channel_close(ec, g->outgoing_channel);
    ractor_channel_close(ec, g->incoming_channel);
}

void
rb_ractor_recv_parameters(rb_execution_context_t *ec, rb_ractor_t *g, int len, VALUE *ptr)
{
    for (int i=0; i<len; i++) {
        ptr[i] = ractor_channel_recv(ec, g->incoming_channel);
    }
}

void
rb_ractor_send_parameters(rb_execution_context_t *ec, rb_ractor_t *g, VALUE args)
{
    int len = RARRAY_LENINT(args);
    for (int i=0; i<len; i++) {
        ractor_channel_send(ec, g->incoming_channel, RARRAY_AREF(args, i));
    }
}

static rb_ractor_channel_t*
ractor_channel(VALUE gcv)
{
    if (rb_ractor_p(gcv)) {
        return RACTOR_CHANNEL_PTR(RACTOR_PTR(gcv)->outgoing_channel);
    }
    else if (rb_ractor_channel_p(gcv)) {
        return RACTOR_CHANNEL_PTR(gcv);
    }
    else {
        rb_bug("unreachable");
    }
}

static VALUE
ractor_select(rb_execution_context_t *ec, VALUE chs)
{
    rb_ractor_t *g = rb_ec_ractor_ptr(ec);
    int chs_len = RARRAY_LENINT(chs);
    int i;

    while (1) {
        // try // TODO: this order should be shuffle
        for (i=0; i<chs_len; i++) {
            VALUE gcv = RARRAY_AREF(chs, i);
            rb_ractor_channel_t *gc = ractor_channel(gcv);
            VALUE v;

            if ((v = ractor_channel_try_recv(ec, gc)) != Qundef) {
                return rb_ary_new_from_args(2, gcv, v);
            }
        }

        ractor_sleep_setup(ec, g);

        // register waiters
        for (i=0; i<chs_len; i++) {
            VALUE gcv = RARRAY_AREF(chs, i);
            rb_ractor_channel_t *gc = ractor_channel(gcv);
            ractor_channel_waiting_add(gc, g);
        }

        ractor_sleep(ec, g);

        // remove waiters
        for (i=0; i<chs_len; i++) {
            VALUE gcv = RARRAY_AREF(chs, i);
            rb_ractor_channel_t *gc = ractor_channel(gcv);
            ractor_channel_waiting_del(gc, g);
        }
    }
}

#include "ractor.rbinc"

static VALUE
ractor_moved_missing(int argc, VALUE *argv, VALUE self)
{
    rb_raise(rb_eRactorChannelError, "can not send any methods to a moved object");
}

void
Init_Ractor(void)
{
    rb_cRactor = rb_define_class("Ractor", rb_cObject);

    rb_cRactorChannel = rb_define_class_under(rb_cRactor, "Channel", rb_cObject);
    rb_undef_alloc_func(rb_cRactorChannel);
    rb_define_singleton_method(rb_cRactorChannel, "new", ractor_channel_new, 0);

    rb_eRactorRemoteError = rb_define_class_under(rb_cRactor, "RemoteError", rb_eRuntimeError);

    rb_eRactorChannelClosedError = rb_define_class_under(rb_cRactorChannel, "ClosedError", rb_eRuntimeError);
    rb_eRactorChannelError = rb_define_class_under(rb_cRactorChannel, "Error", rb_eRuntimeError);

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
