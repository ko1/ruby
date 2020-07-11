#include "ruby/ruby.h"
#include "vm_core.h"
#include "id_table.h"
#include "vm_debug.h"

#ifndef RACTOR_CHECK_MODE
#define RACTOR_CHECK_MODE (0 || VM_CHECK_MODE || RUBY_DEBUG)
#endif

enum rb_ractor_basket_type {
    basket_type_none,
    basket_type_shareable,
    basket_type_copy_marshal,
    basket_type_copy_custom,
    basket_type_move,
    basket_type_exception,
};

struct rb_ractor_basket {
    enum rb_ractor_basket_type type;
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
    // ractor lock
    rb_nativethread_lock_t lock;
#if RACTOR_CHECK_MODE > 0
    VALUE locked_by;
#endif

    // communication
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
            wakeup_by_retry,
        } wakeup_status;

        struct rb_ractor_basket taken_basket;
        struct rb_ractor_basket yielded_basket;

        rb_nativethread_cond_t cond;
    } wait;

    // vm wide barrier synchronization
    rb_nativethread_cond_t barrier_wait_cond;

    // thread management
    struct {
        struct list_head set;
        unsigned int cnt;
        unsigned int blocking_cnt;
        unsigned int sleeper;
        rb_global_vm_lock_t gvl;

        rb_thread_t *running;
        rb_thread_t *main;
    } threads;
    VALUE thgroup_default;

    // identity
    VALUE self;
    uint32_t id;
    VALUE name;
    VALUE loc;

    // created
    //   | ready to run
    // ====================== inserted to vm->ractor
    //   v
    // blocking <---+ all threads are blocking
    //   |          |
    //   v          |
    // running -----+
    //   | all threads are terminated.
    // ====================== removed from vm->ractor
    //   v
    // terminated
    //
    // status is protected by VM lock (global state)

    enum ractor_status {
        ractor_created,
        ractor_running,
        ractor_blocking,
        ractor_terminated,
    } status_;

    struct list_node vmlr_node;
}; // rb_ractor_t is defined in vm_core.h

rb_ractor_t *rb_ractor_main_alloc(void);
void rb_ractor_main_setup(rb_vm_t *vm, rb_ractor_t *main_ractor, rb_thread_t *main_thread);
int rb_ractor_main_p(void);
VALUE rb_ractor_self(const rb_ractor_t *g);
void rb_ractor_atexit(rb_execution_context_t *ec, VALUE result);
void rb_ractor_atexit_exception(rb_execution_context_t *ec);
void rb_ractor_teardown(rb_execution_context_t *ec);
void rb_ractor_recv_parameters(rb_execution_context_t *ec, rb_ractor_t *g, int len, VALUE *ptr);
void rb_ractor_send_parameters(rb_execution_context_t *ec, rb_ractor_t *g, VALUE args);


VALUE rb_thread_create_ractor(rb_ractor_t *g, VALUE args, VALUE proc); // defined in thread.c

rb_global_vm_lock_t *rb_ractor_gvl(rb_ractor_t *);
int rb_ractor_living_thread_num(const rb_ractor_t *);
VALUE rb_ractor_thread_list(rb_ractor_t *r);

void rb_ractor_living_threads_init(rb_ractor_t *r);
void rb_ractor_living_threads_insert(rb_ractor_t *r, rb_thread_t *th);
void rb_ractor_living_threads_remove(rb_ractor_t *r, rb_thread_t *th);
void rb_ractor_blocking_threads_inc(rb_ractor_t *r, const char *file, int line); // TODO: file, line only for RUBY_DEBUG_LOG
void rb_ractor_blocking_threads_dec(rb_ractor_t *r, const char *file, int line); // TODO: file, line only for RUBY_DEBUG_LOG

void rb_ractor_vm_barrier_interrupt_running_thread(rb_ractor_t *r);
void rb_ractor_terminate_interrupt_main_thread(rb_ractor_t *r);
void rb_ractor_terminate_all(void);

static inline bool
rb_ractor_status_p(rb_ractor_t *r, enum ractor_status status)
{
    return r->status_ == status;
}

static inline void
rb_ractor_sleeper_threads_inc(rb_ractor_t *r)
{
    r->threads.sleeper++;
}

static inline void
rb_ractor_sleeper_threads_dec(rb_ractor_t *r)
{
    r->threads.sleeper--;
}

static inline void
rb_ractor_sleeper_threads_clear(rb_ractor_t *r)
{
    r->threads.sleeper = 0;
}

static inline int
rb_ractor_sleeper_thread_num(rb_ractor_t *r)
{
    return r->threads.sleeper;
}

static inline void
rb_thread_set_current_raw(const rb_thread_t *th)
{
    rb_ec_set_current_raw(th->ec);
}

static inline void
rb_thread_set_current(rb_thread_t *th)
{
    VM_ASSERT(th->ractor == GET_RACTOR());

    if (th->ractor->threads.running != th) {
        th->running_time_us = 0;
    }
    rb_thread_set_current_raw(th);
    th->ractor->threads.running = th;
}

void rb_vm_ractor_blocking_cnt_inc(rb_vm_t *vm, rb_ractor_t *cr, const char *file, int line);
void rb_vm_ractor_blocking_cnt_dec(rb_vm_t *vm, rb_ractor_t *cr, const char *file, int line);

// TODO: deep frozen
#define RB_OBJ_SHAREABLE_P(obj) FL_TEST_RAW((obj), RUBY_FL_SHAREABLE)

bool rb_ractor_shareable_p_continue(VALUE obj);

static inline bool
rb_ractor_shareable_p(VALUE obj)
{
    if (SPECIAL_CONST_P(obj)) {
        return true;
    }
    else if (RB_OBJ_SHAREABLE_P(obj)) {
        return true;
    }
    else {
        return rb_ractor_shareable_p_continue(obj);
    }
}

uint32_t rb_ractor_id(const rb_ractor_t *r);

#if RACTOR_CHECK_MODE > 0
uint32_t rb_ractor_current_id(void);

static inline void
rb_ractor_setup_belonging_to(VALUE obj, uint32_t rid)
{
    VALUE flags = RBASIC(obj)->flags & 0xffffffff; // 4B
    RBASIC(obj)->flags = flags | ((VALUE)rid << 32);
}

static inline void
rb_ractor_setup_belonging(VALUE obj)
{
    rb_ractor_setup_belonging_to(obj, rb_ractor_current_id());
}

static inline uint32_t
rb_ractor_belonging(VALUE obj)
{
    if (rb_ractor_shareable_p(obj)) {
        return 0;
    }
    else {
        return RBASIC(obj)->flags >> 32;
    }
}

static inline VALUE
rb_ractor_confirm_belonging(VALUE obj)
{
    uint32_t id = rb_ractor_belonging(obj);

    if (id == 0) {
        if (!rb_ractor_shareable_p(obj)) {
            rp(obj);
            rb_bug("id == 0 but not shareable");
        }
    }
    else if (id != rb_ractor_current_id()) {
        rb_bug("rb_ractor_confirm_belonging object-ractor id:%u, current-ractor id:%u", id, rb_ractor_current_id());
    }
    return obj;
}
#else
#define rb_ractor_confirm_belonging(obj) obj
#endif
