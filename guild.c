// Guild implementation

#include "ruby/ruby.h"
#include "ruby/thread.h"
#include "ruby/thread_native.h"
#include "vm_core.h"
#include "guild.h"

static VALUE rb_cGuild;
static VALUE rb_eGuildRemoteError;

static VALUE rb_cGuildChannel;
static VALUE rb_eGuildChannelClosedError;
static VALUE rb_eGuildChannelError;
static VALUE rb_cGuildMovedObject;

typedef struct rb_guild_struct {
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
} rb_guild_t;

enum rb_guild_channel_basket_type {
    basket_type_shareable,
    basket_type_copy_marshal,
    basket_type_copy_custom,
    basket_type_move,
    basket_type_exception,
};

struct rb_guild_channel_basket {
    int type;
    VALUE v;
    VALUE sender;
};

typedef struct rb_guild_channel_struct {
    struct rb_guild_channel_basket *baskets;
    int cnt;
    int size;

    rb_nativethread_lock_t lock;

    struct channel_waitings {
        int cnt;
        int size;
        rb_guild_t **guilds;
    } waiting;

    bool closed;
} rb_guild_channel_t;

static void
guild_mark(void *ptr)
{
    // fprintf(stderr, "%s:%p\n", __func__, ptr);
    rb_guild_t *g = (rb_guild_t *)ptr;

    rb_gc_mark(g->incoming_channel);
    rb_gc_mark(g->outgoing_channel);
    rb_gc_mark(g->running_thread);
    rb_gc_mark(g->loc);
    rb_gc_mark(g->name);
}

static void
guild_free(void *ptr)
{
    // fprintf(stderr, "%s:%p\n", __func__, ptr);

    rb_guild_t *g = (rb_guild_t *)ptr;
    rb_native_mutex_destroy(&g->sleep_lock);
    rb_native_cond_destroy(&g->sleep_cond);
}

static size_t
guild_memsize(const void *ptr)
{
    return sizeof(rb_guild_t);
}

static const rb_data_type_t guild_data_type = {
    "guild",
    {
        guild_mark,
	guild_free,
        guild_memsize,
        NULL, // update
    },
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY /* | RUBY_TYPED_WB_PROTECTED */
};

static void
guild_channel_mark(void *ptr)
{
    // fprintf(stderr, "%s:%p\n", __func__, ptr);
    rb_guild_channel_t *gc = (rb_guild_channel_t *)ptr;
    for (int i=0; i<gc->cnt; i++) {
        rb_gc_mark(gc->baskets[i].v);
        rb_gc_mark(gc->baskets[i].sender);
    }
}

static void
guild_channel_free(void *ptr)
{
    // fprintf(stderr, "%s:%p\n", __func__, ptr);
    rb_guild_channel_t *gc = (rb_guild_channel_t *)ptr;
    ruby_xfree(gc->waiting.guilds);
    rb_native_mutex_destroy(&gc->lock);
}

static size_t
guild_channel_memsize(const void *ptr)
{
    // TODO
    return sizeof(rb_guild_channel_t);
}

static const rb_data_type_t guild_channel_data_type = {
    "guild/channel",
    {
        guild_channel_mark,
	guild_channel_free,
        guild_channel_memsize,
        NULL, // update
    },
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY /* | RUBY_TYPED_WB_PROTECTED */
};

bool
rb_guild_p(VALUE gv)
{
    if (rb_typeddata_is_kind_of(gv, &guild_data_type)) {
        return true;
    }
    else {
        return false;
    }
}

bool
rb_guild_channel_p(VALUE gcv)
{
    if (rb_typeddata_is_kind_of(gcv, &guild_channel_data_type)) {
        return true;
    }
    else {
        return false;
    }
}

static inline rb_guild_t *
GUILD_PTR(VALUE self)
{
    VM_ASSERT(rb_guild_p(self));
    rb_guild_t *g = DATA_PTR(self);
    // TODO: check
    return g;
}

static inline rb_guild_channel_t *
GUILD_CHANNEL_PTR(VALUE self)
{
    VM_ASSERT(rb_guild_channel_p(self));
    rb_guild_channel_t *gc = DATA_PTR(self);
    // TODO: check
    return gc;
}

uint32_t
rb_guild_id(const rb_guild_t *g)
{
    return g->id;
}

static uint32_t guild_last_id;

#if GUILD_CHECK_MODE > 0
MJIT_FUNC_EXPORTED uint32_t
rb_guild_current_id(void)
{
    if (GET_THREAD()->guild == NULL) {
        return 1; // main guild
    }
    else {
        return GET_GUILD()->id;
    }
}
#endif

static VALUE
guild_channel_alloc(VALUE klass)
{
    rb_guild_channel_t *gc;
    VALUE gcv = TypedData_Make_Struct(klass, rb_guild_channel_t, &guild_channel_data_type, gc);
    FL_SET_RAW(gcv, RUBY_FL_SHAREABLE);

    gc->size = 2;
    gc->cnt = 0;
    gc->baskets = ALLOC_N(struct rb_guild_channel_basket, gc->size);

    gc->waiting.cnt = 0;
    gc->waiting.size = 0;
    gc->waiting.guilds = NULL;

    rb_native_mutex_initialize(&gc->lock);
    return gcv;
}

static VALUE
guild_channel_create(rb_execution_context_t *ec)
{
    VALUE gcv = guild_channel_alloc(rb_cGuildChannel);
    // rb_guild_channel_t *gc = GUILD_CHANNEL_PTR(gcv);
    return gcv;
}

VALUE rb_newobj_with(VALUE src);

static VALUE
guild_channel_move_new(VALUE obj)
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

    rv->klass = rb_cGuildMovedObject;
    rv->v1 = 0;
    rv->v2 = 0;
    rv->v3 = 0;

    // TODO: record moved location
    // TOOD: check flags for each data types

    return v;
}

static VALUE
guild_channel_move_shallow_copy(VALUE obj)
{
    if (rb_guild_shareable_p(obj)) {
        return obj;
    }
    else {
        switch (BUILTIN_TYPE(obj)) {
          case T_STRING:
          case T_FILE:
            if (!FL_TEST_RAW(obj, RUBY_FL_EXIVAR)) {
                return guild_channel_move_new(obj);
            }
            break;
          case T_ARRAY:
            if (!FL_TEST_RAW(obj, RUBY_FL_EXIVAR)) {
                VALUE ary = guild_channel_move_new(obj);
                long len = RARRAY_LEN(ary);
                for (long i=0; i<len; i++) {
                    VALUE e = RARRAY_AREF(ary, i);
                    RARRAY_ASET(ary, i, guild_channel_move_shallow_copy(e)); // confirm WB
                }
                return ary;
            }
            break;
          default:
            break;
        }

        rb_raise(rb_eGuildChannelError, "can't move this this kind of object:%"PRIsVALUE, obj);
    }
}

static void
guild_channel_move_setup(struct rb_guild_channel_basket *b, VALUE obj)
{
    if (rb_guild_shareable_p(obj)) {
        b->type = basket_type_shareable;
        b->v = obj;
    }
    else {
        b->type = basket_type_move;
        b->v = guild_channel_move_shallow_copy(obj);
        return;
    }
}

static VALUE
guild_channel_moved_setup(VALUE obj)
{
#if GUILD_CHECK_MODE
    switch (BUILTIN_TYPE(obj)) {
      case T_STRING:
      case T_FILE:
        rb_guild_setup_belonging(obj);
        break;
      case T_ARRAY:
        rb_guild_setup_belonging(obj);
        long len = RARRAY_LEN(obj);
        for (long i=0; i<len; i++) {
            VALUE e = RARRAY_AREF(obj, i);
            if (!rb_guild_shareable_p(e)) {
                guild_channel_moved_setup(e);
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
guild_channel_recv_accept(struct rb_guild_channel_basket *b)
{
    switch (b->type) {
      case basket_type_shareable:
        VM_ASSERT(rb_guild_shareable_p(b->v));
        return b->v;
      case basket_type_copy_marshal:
        return rb_marshal_load(b->v);
      case basket_type_exception:
        {
            VALUE cause = rb_marshal_load(b->v);
            VALUE err = rb_exc_new_cstr(rb_eGuildRemoteError, "thrown by remote Guild.");
            rb_ivar_set(err, rb_intern("@guild"), b->sender);
            rb_ec_setup_exception(NULL, err, cause);
            rb_exc_raise(err);
        }
        // unreachable
      case basket_type_move:
        return guild_channel_moved_setup(b->v);
      default:
        rb_bug("unreachable");
    }
}

static void
guild_channel_copy_setup(struct rb_guild_channel_basket *b, VALUE obj)
{
    if (rb_guild_shareable_p(obj)) {
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
guild_channel_try_recv(rb_execution_context_t *ec, rb_guild_channel_t *gc)
{
    struct rb_guild_channel_basket basket;

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
            rb_raise(rb_eGuildChannelClosedError, "The send-edge is already closed");
        }
        else {
            return Qundef;
        }
    }

    return guild_channel_recv_accept(&basket);
}

static void *
guild_sleep_wo_gvl(void *ptr)
{
    rb_guild_t *g = ptr;
    rb_native_mutex_lock(&g->sleep_lock);
    if (g->sleep_interrupted == false) {
        rb_native_cond_wait(&g->sleep_cond, &g->sleep_lock);
    }
    rb_native_mutex_unlock(&g->sleep_lock);
    return NULL;
}

static void
guild_sleep_cancel(void *ptr)
{
    rb_guild_t *g = ptr;
    rb_native_mutex_lock(&g->sleep_lock);
    rb_native_cond_signal(&g->sleep_cond);
    rb_native_mutex_unlock(&g->sleep_lock);
}

static void
guild_sleep_setup(rb_execution_context_t *ec, rb_guild_t *g)
{
    rb_native_mutex_lock(&g->sleep_lock);
    {
        g->sleep_interrupted = false;
    }
    rb_native_mutex_unlock(&g->sleep_lock);
}

static void
guild_sleep(rb_execution_context_t *ec, rb_guild_t *g)
{
    rb_native_mutex_lock(&g->sleep_lock);
    {
        while (g->sleep_interrupted == false) {
            rb_native_mutex_unlock(&g->sleep_lock);

            rb_thread_call_without_gvl(guild_sleep_wo_gvl, g,
                                       guild_sleep_cancel, g);

            rb_native_mutex_lock(&g->sleep_lock);
        }
    }
    rb_native_mutex_unlock(&g->sleep_lock);
}

static bool
guild_channel_waiting_p(rb_guild_channel_t *gc)
{
    // TODO: assert(gc->lock is locked)
    return gc->waiting.cnt > 0;
}

static void
guild_channel_waiting_add(rb_guild_channel_t *gc, rb_guild_t *g)
{
    rb_native_mutex_lock(&gc->lock);
    {
        for (int i=0; i<gc->waiting.cnt; i++) {
            if (gc->waiting.guilds[i] == g) {
                // TODO: make it clean code.
                rb_native_mutex_unlock(&gc->lock);
                rb_raise(rb_eRuntimeError, "Already another thread of same guild is waiting.");
            }
        }

        if (gc->waiting.size == 0) {
            gc->waiting.size = 4;
            gc->waiting.guilds = ALLOC_N(rb_guild_t *, gc->waiting.size);
        }
        else if (gc->waiting.size <= gc->waiting.cnt + 1) {
            gc->waiting.size *= 2;
            REALLOC_N(gc->waiting.guilds, rb_guild_t *, gc->waiting.size);
        }
        gc->waiting.guilds[gc->waiting.cnt++] = g;
        // fprintf(stderr, "cnt:%d size:%d\n", gc->waiting.cnt, gc->waiting.size);
    }
    rb_native_mutex_unlock(&gc->lock);
}

static void
guild_channel_waiting_del(rb_guild_channel_t *gc, rb_guild_t *g)
{
    rb_native_mutex_lock(&gc->lock);
    {
        int pos = -1;
        for (int i=0; i<gc->waiting.cnt; i++) {
            if (gc->waiting.guilds[i] == g) {
                pos = i;
                break;
            }
        }
        if (pos >= 0) { // found
            gc->waiting.cnt--;
            for (int i=pos; i<gc->waiting.cnt; i++) {
                gc->waiting.guilds[i] = gc->waiting.guilds[i+1];
            }
        }
    }
    rb_native_mutex_unlock(&gc->lock);
}

static VALUE
guild_channel_recv(rb_execution_context_t *ec, VALUE gcv)
{
    rb_guild_t *g = rb_ec_guild_ptr(ec);
    rb_guild_channel_t *gc = GUILD_CHANNEL_PTR(gcv);
    VALUE v;

    while ((v = guild_channel_try_recv(ec, gc)) == Qundef) {
        guild_sleep_setup(ec, g);
        guild_channel_waiting_add(gc, g);
        guild_sleep(ec, g);
        guild_channel_waiting_del(gc, g);
    }

    RB_GC_GUARD(gcv);
    return v;
}

static bool
guild_channel_wakeup(rb_guild_channel_t *gc, rb_guild_t *wg)
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
guild_channel_wakeup_all(rb_guild_channel_t *gc)
{
    // TODO: assert(gc->lock is locked)
    for (int i=0; i<gc->waiting.cnt; i++) {
        rb_guild_t *wg = gc->waiting.guilds[i];
        guild_channel_wakeup(gc, wg);
    }
}

static void
guild_channel_send_basket(rb_execution_context_t *ec, rb_guild_channel_t *gc, struct rb_guild_channel_basket *b)
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
                REALLOC_N(gc->baskets, struct rb_guild_channel_basket, gc->size);
            }
            b->sender = rb_ec_guild_ptr(ec)->self;
            gc->baskets[gc->cnt++] = *b;

            if (guild_channel_waiting_p(gc)) {
                guild_channel_wakeup_all(gc);
            }
        }
    }
    rb_native_mutex_unlock(&gc->lock);

    if (closed) {
        rb_raise(rb_eGuildChannelClosedError, "The recv-edge is already closed");
    }
}

static VALUE
guild_channel_send_exception(rb_execution_context_t *ec, VALUE gcv, VALUE errinfo)
{
    rb_guild_channel_t *gc = GUILD_CHANNEL_PTR(gcv);
    struct rb_guild_channel_basket basket;
    guild_channel_copy_setup(&basket, errinfo);
    basket.type = basket_type_exception;

    guild_channel_send_basket(ec, gc, &basket);

    return gcv;
}

static VALUE
guild_channel_send(rb_execution_context_t *ec, VALUE gcv, VALUE obj)
{
    rb_guild_channel_t *gc = GUILD_CHANNEL_PTR(gcv);
    struct rb_guild_channel_basket basket;
    guild_channel_copy_setup(&basket, obj);
    guild_channel_send_basket(ec, gc, &basket);
    return gcv;
}

static VALUE
guild_channel_move(rb_execution_context_t *ec, VALUE gcv, VALUE obj)
{
    rb_guild_channel_t *gc = GUILD_CHANNEL_PTR(gcv);
    struct rb_guild_channel_basket basket;
    guild_channel_move_setup(&basket, obj);

    guild_channel_send_basket(ec, gc, &basket);

    return gcv;
}

static VALUE
guild_channel_close(rb_execution_context_t *ec, VALUE gcv)
{
    rb_guild_channel_t *gc = GUILD_CHANNEL_PTR(gcv);
    VALUE prev;

    rb_native_mutex_lock(&gc->lock);
    {
        if (!gc->closed) {
            prev = Qfalse;
            gc->closed = true;

            if (guild_channel_waiting_p(gc)) {
                guild_channel_wakeup_all(gc);
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
guild_channel_new(VALUE self)
{
    return guild_channel_alloc(self);
}

static VALUE
guild_next_id(void)
{
    // TODO: lock
    return ++guild_last_id;
}

static void
guild_setup(rb_guild_t *g)
{
    rb_execution_context_t *ec = GET_EC();
    g->incoming_channel = guild_channel_create(ec);
    g->outgoing_channel = guild_channel_create(ec);
    rb_native_mutex_initialize(&g->sleep_lock);
    rb_native_cond_initialize(&g->sleep_cond);
}

static VALUE
guild_alloc(VALUE klass)
{
    rb_guild_t *g;
    VALUE gv = TypedData_Make_Struct(klass, rb_guild_t, &guild_data_type, g);
    FL_SET_RAW(gv, RUBY_FL_SHAREABLE);

    // namig
    g->id = guild_next_id();
    g->loc = Qnil;
    g->name = Qnil;
    g->self = gv;
    guild_setup(g);
    return gv;
}

rb_guild_t *
rb_guild_main_alloc(void)
{
    rb_guild_t *g = ruby_mimmalloc(sizeof(rb_guild_t));
    if (g == NULL) {
	fprintf(stderr, "[FATAL] failed to allocate memory for main guild\n");
        exit(EXIT_FAILURE);
    }
    MEMZERO(g, rb_guild_t, 1);
    g->id = ++guild_last_id;
    g->loc = Qnil;
    g->name = Qnil;

    return g;
}

void
rb_guild_main_setup(rb_guild_t *g)
{
    g->self = TypedData_Wrap_Struct(rb_cGuild, &guild_data_type, g);
    guild_setup(g);
}


VALUE
rb_guild_self(const rb_guild_t *g)
{
    return g->self;
}

MJIT_FUNC_EXPORTED int
rb_guild_main_p(void)
{
    rb_execution_context_t *ec = GET_EC();
    return rb_ec_guild_ptr(ec) == rb_ec_vm_ptr(ec)->main_guild;
}


static VALUE
guild_create(rb_execution_context_t *ec, VALUE self,
             VALUE args, VALUE block, VALUE loc, VALUE name)
{
    VALUE gv = guild_alloc(self);
    rb_guild_t *g = GUILD_PTR(gv);
    g->running_thread = rb_thread_create_guild(g, args, block);
    g->loc = loc;
    g->name = name;
    return gv;
}

void
rb_guild_atexit(rb_execution_context_t *ec, VALUE result)
{
    rb_guild_t *g = rb_ec_guild_ptr(ec);
    guild_channel_send(ec, g->outgoing_channel, result);
    guild_channel_close(ec, g->outgoing_channel);
    guild_channel_close(ec, g->incoming_channel);
}

void
rb_guild_atexit_exception(rb_execution_context_t *ec)
{
    rb_guild_t *g = rb_ec_guild_ptr(ec);
    guild_channel_send_exception(ec, g->outgoing_channel, ec->errinfo);
    guild_channel_close(ec, g->outgoing_channel);
    guild_channel_close(ec, g->incoming_channel);
}

void
rb_guild_recv_parameters(rb_execution_context_t *ec, rb_guild_t *g, int len, VALUE *ptr)
{
    for (int i=0; i<len; i++) {
        ptr[i] = guild_channel_recv(ec, g->incoming_channel);
    }
}

void
rb_guild_send_parameters(rb_execution_context_t *ec, rb_guild_t *g, VALUE args)
{
    int len = RARRAY_LENINT(args);
    for (int i=0; i<len; i++) {
        guild_channel_send(ec, g->incoming_channel, RARRAY_AREF(args, i));
    }
}

static rb_guild_channel_t*
guild_channel(VALUE gcv)
{
    if (rb_guild_p(gcv)) {
        return GUILD_CHANNEL_PTR(GUILD_PTR(gcv)->outgoing_channel);
    }
    else if (rb_guild_channel_p(gcv)) {
        return GUILD_CHANNEL_PTR(gcv);
    }
    else {
        rb_bug("unreachable");
    }
}

static VALUE
guild_select(rb_execution_context_t *ec, VALUE chs)
{
    rb_guild_t *g = rb_ec_guild_ptr(ec);
    int chs_len = RARRAY_LENINT(chs);
    int i;

    while (1) {
        // try // TODO: this order should be shuffle
        for (i=0; i<chs_len; i++) {
            VALUE gcv = RARRAY_AREF(chs, i);
            rb_guild_channel_t *gc = guild_channel(gcv);
            VALUE v;

            if ((v = guild_channel_try_recv(ec, gc)) != Qundef) {
                return rb_ary_new_from_args(2, gcv, v);
            }
        }

        guild_sleep_setup(ec, g);

        // register waiters
        for (i=0; i<chs_len; i++) {
            VALUE gcv = RARRAY_AREF(chs, i);
            rb_guild_channel_t *gc = guild_channel(gcv);
            guild_channel_waiting_add(gc, g);
        }

        guild_sleep(ec, g);

        // remove waiters
        for (i=0; i<chs_len; i++) {
            VALUE gcv = RARRAY_AREF(chs, i);
            rb_guild_channel_t *gc = guild_channel(gcv);
            guild_channel_waiting_del(gc, g);
        }
    }
}

#include "guild.rbinc"

static VALUE
guild_moved_missing(int argc, VALUE *argv, VALUE self)
{
    rb_raise(rb_eGuildChannelError, "can not send any methods to a moved object");
}

void
Init_Guild(void)
{
    rb_cGuild = rb_define_class("Guild", rb_cObject);

    rb_cGuildChannel = rb_define_class_under(rb_cGuild, "Channel", rb_cObject);
    rb_undef_alloc_func(rb_cGuildChannel);
    rb_define_singleton_method(rb_cGuildChannel, "new", guild_channel_new, 0);

    rb_eGuildRemoteError = rb_define_class_under(rb_cGuild, "RemoteError", rb_eRuntimeError);

    rb_eGuildChannelClosedError = rb_define_class_under(rb_cGuildChannel, "ClosedError", rb_eRuntimeError);
    rb_eGuildChannelError = rb_define_class_under(rb_cGuildChannel, "Error", rb_eRuntimeError);

    rb_cGuildMovedObject = rb_define_class_under(rb_cGuild, "MovedObject", rb_cBasicObject);
    rb_undef_alloc_func(rb_cGuildMovedObject);
    rb_define_method(rb_cGuildMovedObject, "method_missing", guild_moved_missing, -1);

    // override methods defined in BasicObject
    rb_define_method(rb_cGuildMovedObject, "__send__", guild_moved_missing, -1);
    rb_define_method(rb_cGuildMovedObject, "!", guild_moved_missing, -1);
    rb_define_method(rb_cGuildMovedObject, "==", guild_moved_missing, -1);
    rb_define_method(rb_cGuildMovedObject, "!=", guild_moved_missing, -1);
    rb_define_method(rb_cGuildMovedObject, "__id__", guild_moved_missing, -1);
    rb_define_method(rb_cGuildMovedObject, "equal?", guild_moved_missing, -1);
    rb_define_method(rb_cGuildMovedObject, "instance_eval", guild_moved_missing, -1);
    rb_define_method(rb_cGuildMovedObject, "instance_exec", guild_moved_missing, -1);

    rb_obj_freeze(rb_cGuildMovedObject);
}

MJIT_FUNC_EXPORTED bool
rb_guild_shareable_p_continue(VALUE obj)
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
