#include "ruby/ruby.h"
#include "vm_core.h"
#include "id_table.h"

#ifndef RACTOR_CHECK_MODE
#define RACTOR_CHECK_MODE (1 || RUBY_DEBUG)
#endif

rb_ractor_t *rb_ractor_main_alloc(void);
void rb_ractor_main_setup(rb_ractor_t *main_ractor);

VALUE rb_ractor_self(const rb_ractor_t *g);
void rb_ractor_atexit(rb_execution_context_t *ec, VALUE result);
void rb_ractor_atexit_exception(rb_execution_context_t *ec);
void rb_ractor_recv_parameters(rb_execution_context_t *ec, rb_ractor_t *g, int len, VALUE *ptr);
void rb_ractor_send_parameters(rb_execution_context_t *ec, rb_ractor_t *g, VALUE args);

int rb_ractor_main_p(void);

VALUE rb_thread_create_ractor(rb_ractor_t *g, VALUE args, VALUE proc); // defined in thread.c

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

#if RACTOR_CHECK_MODE > 0

uint32_t rb_ractor_id(const rb_ractor_t *r);
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
