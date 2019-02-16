#include "ruby/ruby.h"
#include "vm_core.h"
#include "vm_debug.h"
#include "ruby/thread.h"
#include <pthread.h>
#include "thread_native.h"

/* vm_trace.c */
void rb_vm_trace_mark_event_hooks(rb_hook_list_t *hooks);

VALUE rb_cGuild;
VALUE rb_cGuildChannel;

VALUE rb_guild_channel_create(void);
static VALUE guild_channel_copy(VALUE obj);

/* Guild internals */

struct guild_channel {
    VALUE *q;
    int q_capa, q_size, q_head, q_tail;
    pthread_mutex_t lock;
    pthread_cond_t cond;
};

static void
guild_mark(void *ptr)
{
    if (ptr) {
	rb_guild_t *g = ptr;
        rb_thread_t *th;

        if (GUILD_DEBUG) fprintf(stderr, "%d: guild_mark (%d:%p)\n", GET_GUILD()->id, g->id, g);

        /* mark living threads */
        list_for_each(&g->living_threads, th, lt_node) {
            if (GUILD_DEBUG) fprintf(stderr, "  * guild_mark thread (g:%d th:%p)\n", g->id, th);
            rb_gc_mark(th->self);
        }

        /* mark others */
        rb_gc_mark(g->default_channel);
        rb_gc_mark(g->thgroup_default);
        rb_vm_trace_mark_event_hooks(&g->event_hooks);
    }
}

static void
guild_free(void *ptr)
{
    if (ptr) {
        rb_guild_t *g = ptr;
	xfree(g);

        if (GUILD_DEBUG) fprintf(stderr, "guild: free %d\n", g->id);
    }
}

static const rb_data_type_t guild_data_type = {
    "guild",
    {guild_mark, guild_free, NULL,},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static void
guild_channel_mark(void *ptr)
{
    if (ptr) {
	struct guild_channel *ch = ptr;
	if (ch->q_tail < ch->q_head) {
	    rb_gc_mark_locations(&ch->q[ch->q_tail], &ch->q[ch->q_head]);
	}
	else {
	    rb_gc_mark_locations(&ch->q[ch->q_tail], &ch->q[ch->q_capa]);
	    rb_gc_mark_locations(&ch->q[0], &ch->q[ch->q_head]);
	}
    }
}

static void
guild_channel_free(void *ptr)
{
    if (ptr) {
	struct guild_channel *ch = ptr;
        if (GUILD_DEBUG) fprintf(stderr, "guild_channel_free\n");
	pthread_cond_destroy(&ch->cond);
	pthread_mutex_destroy(&ch->lock);
    }
}

static const rb_data_type_t guild_channel_data_type = {
    "guild_channel",
    {guild_channel_mark, guild_channel_free, NULL,},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

/* Guild */

VALUE
rb_guild_alloc_wrap(rb_guild_t *g)
{
    VALUE gval = TypedData_Wrap_Struct(rb_cGuild, &guild_data_type, g);
    g->self = gval;
    return gval;
}

void rb_guild_init_postponed_job(rb_guild_t *g);

void
rb_guild_init(rb_guild_t *g, rb_vm_t *vm)
{
    static int id_cnt;

    g->vm = vm;
    rb_guild_living_threads_init(g);
    g->thread_report_on_exception = 1;
    rb_vm_guilds_inserts(vm, g);
    g->id = ++id_cnt;
    rb_guild_init_postponed_job(g);
    if (GUILD_DEBUG) fprintf(stderr, "%d: rb_guild_init (%p)\n", g->id, (void *)pthread_self());
}

VALUE rb_thread_create_core(VALUE thval, VALUE args, VALUE procval, VALUE (*fn)(ANYARGS));

static void
guild_start(rb_guild_t *g, VALUE args)
{
    VALUE thval = rb_thread_alloc(rb_cThread, g);
    rb_thread_create_core(thval, args, rb_proc_isolate(rb_block_proc()), NULL);
    RB_GC_GUARD(thval);
}

void rb_gvl_init(rb_guild_t *g);

static VALUE
guild_new(int argc, VALUE *argv, VALUE self)
{
    VALUE gval;
    rb_guild_t *g = ALLOC_N(rb_guild_t, 1);
    VALUE args = rb_ary_new();
    int i;

    MEMZERO(g, rb_guild_t, 1);

    gval = rb_guild_alloc_wrap(g);
    g->gc_rendezvous.waiting = 3; /* not running */
    g->parent_guild_obj = GET_GUILD()->self;
    rb_guild_init(g, GET_VM());
    RB_OBJ_WRITE(g->self, &g->default_channel, rb_guild_channel_create());
    rb_gvl_init(g);

    for (i=0; i<argc; i++) {
        rb_ary_push(args, guild_channel_copy(argv[i]));
    }
    guild_start(g, args);

    return gval;
}

/* Guild::Channel */

VALUE
rb_guild_channel_create(void)
{
    struct guild_channel *ch;
    VALUE chobj = TypedData_Make_Struct(rb_cGuildChannel, struct guild_channel, &guild_channel_data_type, ch);
    pthread_mutex_init(&ch->lock, NULL);
    pthread_cond_init(&ch->cond, NULL);
    return chobj;
}

#define MIN_Q_CAPA 8

static void
q_stat(struct guild_channel *ch, const char *msg)
{
    if (0)
      fprintf(stderr, "th:%08d - Q: %8s capa:%02d, size:%02d, head:%02d, tail: %02d\n",
              (int)pthread_self(),
              msg, ch->q_capa, ch->q_size, ch->q_head, ch->q_tail);
}

static void
guild_channel_resize(struct guild_channel *ch, int size)
{
    if (ch->q_capa < size) {
	VALUE *old_q = ch->q;
	int i, j;
	int capa = ch->q_capa * 2;
	if (capa < MIN_Q_CAPA) capa = MIN_Q_CAPA;
	q_stat(ch, "resize");
	ch->q = malloc(capa * sizeof(VALUE));
	for (i=0, j=ch->q_tail; i<ch->q_size; i++, j++) {
	    ch->q[i] = old_q[j >= ch->q_capa ? j - ch->q_capa : j];
	}
	free(old_q);
	ch->q_capa = capa;
	ch->q_tail = 0;
	ch->q_head = ch->q_size;
    }
}

static void
guild_channel_push(VALUE chobj, struct guild_channel *ch, VALUE obj)
{
    q_stat(ch, "b/push");
    if (ch->q_size == ch->q_capa) {
	guild_channel_resize(ch, ch->q_size + 1);
    }
    RB_OBJ_WRITE(chobj, &ch->q[ch->q_head], obj);
    ch->q_head++;
    ch->q_size++;
    if (ch->q_head >= ch->q_capa) {
	ch->q_head = 0;
    }
    pthread_cond_signal(&ch->cond);
    q_stat(ch, "a/push");
}

static VALUE
guild_channel_pop(struct guild_channel *ch)
{
    q_stat(ch, "b/pop");
  retry:
    if (ch->q_size == 0) {
	rb_native_cond_wait(&ch->cond, &ch->lock);
	goto retry;
    }
    else {
	VALUE obj = ch->q[ch->q_tail++];
	ch->q_size--;
	if (ch->q_tail == ch->q_capa) {
	    ch->q_tail = 0;
	}
	q_stat(ch, "a/pop");
	return obj;
    }
}

static struct guild_channel *
guild_channel(VALUE self)
{
    /* verify ? */
    return (struct guild_channel *)DATA_PTR(self);
}

#define LOCK_CH(ch) { \
    if (pthread_mutex_lock(&ch->lock) != 0) { \
	perror("mutex_lock"); exit(1); \
    }

#define UNLOCK_CH(ch) \
    if (pthread_mutex_unlock(&ch->lock) != 0) { \
	perror("mutex_unlock"); exit(1); \
    } \
}

static int
guild_channel_deeply_immutable_p(VALUE obj)
{
    if (SPECIAL_CONST_P(obj) ||
	OBJ_FROZEN(obj)) {
	return TRUE;
    }
    else {
	return FALSE;
    }
}

static int
guild_channel_sharable_object_p(VALUE obj)
{
    /* TODO: introduce sharable flag */
    if (rb_typeddata_is_kind_of(obj, &guild_data_type)) {
        return TRUE;
    }
    else {
        return FALSE;
    }
}

static VALUE
guild_channel_copy(VALUE obj)
{
    if (guild_channel_deeply_immutable_p(obj) ||
        guild_channel_sharable_object_p(obj)) {
	return obj;
    }
    else {
	switch (TYPE(obj)) {
	  case T_ARRAY:
	    {
                long i, len = RARRAY_LEN(obj);
		VALUE dst = rb_ary_new_capa(len);

                for (i=0; i<len; i++) {
		    VALUE e = RARRAY_AREF(obj, i);
                    rb_ary_push(dst, guild_channel_copy(e));
		}
		return dst;
	    }
          case T_STRING:
            {
                VALUE dst = rb_str_dup(obj);
                return dst;
            }
          case T_DATA:
            if (rb_obj_is_proc(obj)) {
                VALUE dst = rb_proc_isolate(obj);
                return dst;
            }
            /* fall through */
	  default:
	    rb_raise(rb_eRuntimeError, "can't copy: %"PRIsVALUE, obj);
	}
    }
}

VALUE
rb_guild_channel_transfer_copy(VALUE self, VALUE obj)
{
    struct guild_channel *ch = guild_channel(self);
    VALUE transfer_obj = guild_channel_copy(obj);
    LOCK_CH(ch);
    guild_channel_push(self, ch, transfer_obj);
    UNLOCK_CH(ch);
    return Qnil;
}

static VALUE
guild_channel_move(VALUE obj)
{
    if (guild_channel_deeply_immutable_p(obj)) {
	return obj;
    }
    else {
	switch (BUILTIN_TYPE(obj)) {
	  case T_ARRAY:
	    {
		VALUE dst = rb_newobj_of(RBASIC_CLASS(obj), T_ARRAY | FL_WB_PROTECTED);
		long i, len = RARRAY_LEN(obj);
		if (1) {
		    for (i=0; i<len; i++) {
			VALUE e = RARRAY_AREF(obj, i);
			if (guild_channel_deeply_immutable_p(obj)) {
			    /* do nothing */
			}
			else {
			    RARRAY_ASET(obj, i, guild_channel_move(e));
			}
		    }
		}
		rb_array_replace(dst, obj);
		if (FL_TEST(obj, RARRAY_EMBED_FLAG)) FL_SET(dst, RARRAY_EMBED_FLAG);

		/* reset src obj */
		FL_SET(obj, RARRAY_EMBED_FLAG);
		RBASIC(obj)->flags &= ~RARRAY_EMBED_LEN_MASK;
		RBASIC(obj)->flags |= 0 << RARRAY_EMBED_LEN_SHIFT;

		return dst;
	    }
          case T_STRING:
            {
                VALUE dst = rb_str_dup(obj);
                return dst;
            }
	  default:
	    rb_raise(rb_eRuntimeError, "can't move");
	}
    }
}

VALUE
rb_guild_channel_transfer_move(VALUE self, VALUE obj)
{
    struct guild_channel *ch = guild_channel(self);
    VALUE transfer_obj = guild_channel_move(obj);
    LOCK_CH(ch);
    guild_channel_push(self, ch, transfer_obj);
    UNLOCK_CH(ch);
    return Qnil;
}

VALUE
rb_guild_channel_transfer_reference_danger(VALUE self, VALUE obj)
{
    struct guild_channel *ch = guild_channel(self);
    VALUE transfer_obj = obj;
    LOCK_CH(ch);
    guild_channel_push(self, ch, transfer_obj);
    UNLOCK_CH(ch);
    return Qnil;
}

static void *
guild_channel_pop_wo_gvl(void *data)
{
    struct guild_channel *ch = (struct guild_channel *)data;
    VALUE obj;

    LOCK_CH(ch);
    obj = guild_channel_pop(ch);
    UNLOCK_CH(ch);

    return (void *)obj;
}

VALUE
rb_guild_channel_receive(VALUE self)
{
    struct guild_channel *ch = guild_channel(self);
    void *data;

    data = rb_thread_call_without_gvl(guild_channel_pop_wo_gvl, ch, NULL, NULL);
    // TODO: mark self.
    return (VALUE)data;
}

static VALUE
guild_default_channel(VALUE self)
{
    /* TODO: check */
    VALUE default_ch = ((rb_guild_t *)DATA_PTR(self))->default_channel;
    return default_ch;
}

static VALUE
guild_transfer_copy(VALUE self, VALUE obj)
{
    return rb_guild_channel_transfer_copy(guild_default_channel(self), obj);
}

static VALUE
guild_transfer_move(VALUE self, VALUE obj)
{
    return rb_guild_channel_transfer_move(guild_default_channel(self), obj);
}

static VALUE
guild_transfer_reference_danger(VALUE self, VALUE obj)
{
    return rb_guild_channel_transfer_reference_danger(guild_default_channel(self), obj);
}

static VALUE
guild_receive(VALUE self)
{
    return rb_guild_channel_receive(guild_default_channel(self));
}

/* utility functions */

static VALUE
guild_current(void)
{
    return GET_GUILD()->self;
}

static VALUE
guild_parent(void)
{
    return GET_GUILD()->parent_guild_obj;
}

static VALUE
guild_s_current(VALUE klass)
{
    return guild_current();
}

static VALUE
guild_s_parent(VALUE klass)
{
    return guild_parent();
}

static VALUE
guild_s_list(VALUE self)
{
    rb_vm_t *vm = GET_VM();
    rb_guild_t *g;
    VALUE ary = rb_ary_new();

    list_for_each(&vm->guilds, g, guild_node) {
        rb_ary_push(ary, g->self);
    }

    return ary;
}

static VALUE
guild_s_receive(VALUE klass)
{
    return rb_guild_channel_receive(guild_default_channel(guild_current()));
}

static VALUE
guild_s_yield(VALUE klass, VALUE obj)
{
    VALUE parent_g = guild_parent();
    VALUE ch = guild_default_channel(parent_g);
    return rb_guild_channel_transfer_copy(ch, obj);
}

void
Init_Guild(void)
{
    rb_cGuild = rb_define_class("Guild", rb_cObject);
    rb_cGuildChannel = rb_define_class_under(rb_cGuild, "Channel", rb_cObject);
    rb_undef_alloc_func(rb_cGuildChannel);

    rb_define_singleton_method(rb_cGuildChannel, "create", rb_guild_channel_create, 0);
    //rb_define_singleton_method(rb_cGuildChannel, "transfer_ownership", rb_guild_default_channel_transfer_ownership, 0);
    //rb_define_singleton_method(rb_cGuildChannel, "transfer_copy", rb_guild_default_channel_transfer_copy, 0);

    rb_define_method(rb_cGuildChannel, "transfer_copy", rb_guild_channel_transfer_copy, 1);
    rb_define_method(rb_cGuildChannel, "transfer_move", rb_guild_channel_transfer_move, 1);
    rb_define_method(rb_cGuildChannel, "transfer_reference_danger", rb_guild_channel_transfer_reference_danger, 1);
    rb_define_method(rb_cGuildChannel, "receive", rb_guild_channel_receive, 0);

    rb_define_method(rb_cGuild, "transfer_copy", guild_transfer_copy, 1);
    rb_define_method(rb_cGuild, "transfer_move", guild_transfer_move, 1);
    rb_define_method(rb_cGuild, "transfer_reference_danger", guild_transfer_reference_danger, 1);

    rb_define_method(rb_cGuild, "send", guild_transfer_copy, 1);
    rb_define_method(rb_cGuild, "<<", guild_transfer_copy, 1);
    rb_define_method(rb_cGuild, "move", guild_transfer_move, 1);
    rb_define_method(rb_cGuild, "receive", guild_receive, 0);

    rb_define_singleton_method(rb_cGuild, "new", guild_new, -1);
    rb_define_singleton_method(rb_cGuild, "parent", guild_s_parent, 0);
    rb_define_singleton_method(rb_cGuild, "current", guild_s_current, 0);

    rb_define_singleton_method(rb_cGuild, "receive", guild_s_receive, 0);
    rb_define_singleton_method(rb_cGuild, "list", guild_s_list, 0);
    rb_define_singleton_method(rb_cGuild, "yield", guild_s_yield, 1);
}
