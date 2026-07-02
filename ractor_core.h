#include "internal/gc.h"
#include "ruby/ruby.h"
#include "ruby/ractor.h"
#include "vm_core.h"
#include "id_table.h"
#include "vm_debug.h"

#ifndef RACTOR_CHECK_MODE
#define RACTOR_CHECK_MODE (VM_CHECK_MODE || RUBY_DEBUG) && (SIZEOF_UINT64_T == SIZEOF_VALUE)
#endif

// experimental flag because it is not sure it is the common pattern
#define RUBY_TYPED_FROZEN_SHAREABLE_NO_REC RUBY_FL_FINALIZE

/* RLGCv2 (design_v2.md §4.5): an in-flight move payload, serialized off-heap
 * (defined in ractor.c). */
struct rb_ractor_move_courier;

struct rb_ractor_sync {
    // ractor lock
    rb_nativethread_lock_t lock;

#if RACTOR_CHECK_MODE > 0
    VALUE locked_by;
#endif

#ifndef RUBY_THREAD_PTHREAD_H
    rb_nativethread_cond_t wakeup_cond;
#endif

    // incoming messages
    struct ractor_queue *recv_queue;

    // waiting threads for receiving
    struct ccan_list_head waiters;

    // ports
    VALUE default_port_value;
    struct st_table *ports;
    size_t next_port_id;

    // monitors
    struct ccan_list_head monitors;

    // value
    rb_ractor_t *successor;
    VALUE legacy;
    bool legacy_exc;

    /* RLGCv2 (design_v2.md §4.2): the snapshot currently being
     * materialized by this Ractor's receive. The basket has already been
     * popped from the queue, so this slot is what lets the root scan and
     * the global GC's in-flight re-pin keep the sender-resident snapshot
     * alive while the copy is running. */
    VALUE in_flight_materializing;

    /* RLGCv2 (design_v2.md §4.5): the move courier currently being
     * materialized by this Ractor's receive. The courier is xmalloc'd (not a
     * GC object), so this slot exists only to mark the shareable VALUEs it
     * carries while the rebuild is running. */
    struct rb_ractor_move_courier *in_flight_courier;
};

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
};

struct rb_ractor_struct {
    struct rb_ractor_pub pub;
    struct rb_ractor_sync sync;

    /* objects pinned via rb_gc_register_mark_object; this Ractor owns them and
     * marks them, and hands them to the main Ractor when it terminates. */
    VALUE mark_object_ary;

#if !USE_MODULAR_GC
    /* traversal-API mark redirect (NULL outside a traversal).  Per Ractor so a
     * concurrent traversal on another Ractor is never observed.  A modular GC
     * keeps this in the VM instead (vm->gc.mark_func_data). */
    struct gc_mark_func_data_struct *mark_func_data;
#endif

    // thread management
    struct {
        struct ccan_list_head set;
        unsigned int cnt;
        unsigned int blocking_cnt;
        unsigned int sleeper;
        struct rb_thread_sched sched;
        rb_execution_context_t *running_ec;
        rb_thread_t *main;
    } threads;

    /* Postponed jobs targeted at this Ractor
     * (rb_postponed_job_trigger_for_ractor): bits index the VM-wide
     * preregistration table; any of this Ractor's threads drains them
     * in rb_postponed_job_flush. */
    rb_atomic_t postponed_job_triggered_bits;

    VALUE thgroup_default;

    VALUE name;
    VALUE loc;

    enum ractor_status status_;

    struct ccan_list_node vmlr_node;

    // ractor local data

    rb_serial_t next_ec_serial;

    st_table *local_storage;
    struct rb_id_table *idkey_local_storage;
    VALUE local_storage_store_lock;

    VALUE r_stdin;
    VALUE r_stdout;
    VALUE r_stderr;
    VALUE verbose;
    VALUE debug;

    bool malloc_gc_disabled;
    bool main_ractor;
    void *newobj_cache;

    /* RLGCv2: this Ractor's objspace.  The main Ractor receives the boot
     * objspace in rb_objspace_alloc; non-main Ractors share the main
     * objspace (NULL here) until M1 gives each Ractor its own. */
    void *objspace;

    /* RLGCv2: while this Ractor is creating a child, the child's objspace is
     * already populated (its Thread/Fiber wrappers are born there) but the
     * child is not yet in vm->ractor.set, so a whole-VM walk would miss it. The
     * creator parks the child's objspace here for the window between the wrapper
     * allocation and vm_insert_ractor, so the global GC enumerates it. Per
     * Ractor (concurrent creators each have their own), cleared under the VM
     * lock when the child joins the set. */
    void *creating_child_objspace;

    /* RLGCv2 (design_v2.md section 1.3): the mark redirect installed by
     * this Ractor's object-traversal API call, if any. Per Ractor so
     * that a foreign Ractor's concurrent real GC never sees it; the
     * redirect branch parks it to NULL around each callback, so this
     * Ractor's own real GC (triggered by a callback's allocation)
     * cannot be hijacked either. */
    struct gc_mark_func_data_struct *mark_func_data;

    /* RLGCv2: この Ractor で登録された VM グローバル root を per-Ractor に持つ
     * （旧 vm->global_object_list / vm->mark_object_ary を Ractor-local 化）。
     * これにより local GC は自 Ractor の登録だけを walk し、VM グローバルな
     * 共有リスト（と、その走査を守っていた共有ロック）が hot path から消える。
     * mark は rb_ractor_mark_local_roots（local=current Ractor / global=全 Ractor）。
     *   registered_addrs = rb_gc_register_address（VALUE* の「場所」。*addr を mark_maybe）
     *   registered_marks  = rb_gc_register_mark_object / rb_vm_register_global_object
     *                       （不滅の pinned オブジェクト。mark_vm_stack_values で pin）
     * Ractor 吸収/終了時に継承先へ移管される（rb_ractor_absorb_registered_globals）。 */
    VALUE **registered_addrs;
    size_t registered_addrs_cnt;
    size_t registered_addrs_capa;
    VALUE *registered_marks;
    size_t registered_marks_cnt;
    size_t registered_marks_capa;

    /* RLGCv2: この Ractor が所有する unshareable オブジェクトの generic fields
     * （旧 VM-global な generic_fields_tbl_ + generic_fields_lock を per-Ractor 化）。
     * generic_fields は VM の ivar 格納機能であって GC の機能ではないので、GC-impl の
     * objspace ではなく Ractor に持つ。owner のみが触る（containment）ため無ロック。
     * shareable オブジェクトの分は今も variable.c の global 表 + narrow lock に残る。
     * weak-KEY: key=host obj が死ねば entry は消え、値 fields_obj は live key の strong
     * child。confined GC は per-object の rb_mark_generic_ivar でこの表を引く。global GC
     * は per-object を止め、mark 後に全 Ractor の本表を舐めて drain する（variable.c の
     * rb_gc_vm_generic_fields_* を参照）。lazy に生成する（NULL = まだ空）。
     *
     * この表は owner 専有＝完全無ロック。unshareable オブジェクトは containment により
     * owner=GET_RACTOR() だけが触る。唯一の例外だった Ractor#send の native copy による
     * cross-Ractor read は、送信時に「host→fields_obj の対応表」をメッセージに同梱し
     * （gen_fields_capture / basket->p.gen_fields）、受信側 materialize がそれを引く
     * （gen_fields_materialize）ことで排除した。write は st resize 中の自 Ractor confined
     * GC 再入を避けるため GC 無効化下で行うが、ロックは要らない。 */
    struct st_table *generic_fields_tbl;
    /* RLGCv2: Ractor#send の native copy 中の generic-ivar 対応表。
     *   gen_fields_capturing:  送信側の snapshot 作成中だけ true。generic-ivar host が
     *                          出たとき初めて gen_fields_capture を遅延確保する合図
     *                          （generic ivar 無しのメッセージでは表を確保しない）。
     *   gen_fields_capture:    上の間、copy(snapshot node) が generic-ivar host なら
     *                          その fields_obj をここに記録する（copy_enter）。
     *   gen_fields_materialize: 受信側 materialize 中、rb_obj_fields_generic_uncached が
     *                          自表に無い snapshot host の fields_obj をここから引く。
     * これで受信側が sender の per-Ractor 表を跨がない。 */
    bool gen_fields_capturing;
    struct st_table *gen_fields_capture;
    struct st_table *gen_fields_materialize;
}; // rb_ractor_t is defined in vm_core.h

/* RLGCv2: mark Ractor r's GC roots from its C structure (gc.c root scan). */
void rb_ractor_mark_local_roots(rb_ractor_t *r);
/* rb_ractor_mark_local_roots のうち「登録済み VM グローバル root（registered_addrs /
 * registered_marks）」だけを mark する。これらは object グラフ（ractor_mark）ではなく
 * root なので、Ractor object が到達可能でも root walk で別途 mark が要る。zombie
 * （set から外れたが未 merge。objspace は global GC が sweep する）に対しては、
 * loc/name/threads 等の object-graph 部分は ractor_mark 側（join 待ちなら Ractor
 * object 到達可能、orphan なら回収されるべき）に委ね、ここだけを補う。 */
void rb_ractor_mark_registered_globals(rb_ractor_t *r);
void rb_ractor_repin_in_flight(rb_ractor_t *r);
void rb_ractor_pin_inherited_parts(rb_ractor_t *r);

/* RLGCv2: Ractor-local 化した VM グローバル root（旧 vm->global_object_list /
 * vm->mark_object_ary）の登録・解除・移管。migration は GC sweep（ractor_free）
 * からも呼ばれるので raw malloc/realloc/free のみを使う。 */
void rb_ractor_register_address(rb_ractor_t *r, VALUE *addr);
void rb_ractor_unregister_address(rb_ractor_t *r, VALUE *addr);
void rb_ractor_register_mark_object(rb_ractor_t *r, VALUE obj);
void rb_ractor_absorb_registered_globals(rb_ractor_t *dst, rb_ractor_t *src);

/* RLGCv2: src Ractor の per-Ractor generic_fields 表を dst へ移送して src を空にする
 * （Ractor#value join / orphan free）。実装は variable.c（表のセマンティクスを持つ）。
 * st は raw malloc なので sweep 中の呼び出しも安全。 */
void rb_ractor_absorb_generic_fields(rb_ractor_t *dst, rb_ractor_t *src);
/* RLGCv2: この Ractor の per-Ractor generic_fields 表を解放（ractor_free）。 */
void rb_ractor_free_generic_fields(rb_ractor_t *r);

enum ractor_wakeup_status {
    wakeup_none,
    wakeup_by_send,
    wakeup_by_interrupt,
    // wakeup_by_close,
};

struct ractor_waiter {
    enum ractor_wakeup_status wakeup_status;
    rb_thread_t *th;
    struct ccan_list_node node;
    rb_atomic_t event_serial;
};

static inline VALUE
rb_ractor_self(const rb_ractor_t *r)
{
    return r->pub.self;
}

rb_ractor_t *rb_ractor_main_alloc(void);
void rb_ractor_main_setup(rb_vm_t *vm, rb_ractor_t *main_ractor, rb_thread_t *main_thread);
void rb_vm_ractor_migrate_mark_objects(rb_ractor_t *dst, rb_ractor_t *src);
void rb_ractor_atexit(rb_execution_context_t *ec, VALUE result);
void rb_ractor_atexit_exception(rb_execution_context_t *ec);
void rb_ractor_teardown(rb_execution_context_t *ec);
void rb_ractor_receive_parameters(rb_execution_context_t *ec, rb_ractor_t *g, int len, VALUE *ptr);
void rb_ractor_send_parameters(rb_execution_context_t *ec, rb_ractor_t *g, VALUE args);

VALUE rb_thread_create_ractor(rb_ractor_t *g, VALUE args, VALUE proc); // defined in thread.c

int rb_ractor_living_thread_num(const rb_ractor_t *);
VALUE rb_ractor_thread_list(void);
bool rb_ractor_p(VALUE rv);

void rb_ractor_living_threads_init(rb_ractor_t *r);
void rb_ractor_living_threads_insert(rb_ractor_t *r, rb_thread_t *th);
void rb_ractor_living_threads_remove(rb_ractor_t *r, rb_thread_t *th);
void rb_ractor_blocking_threads_inc(rb_ractor_t *r, const char *file, int line); // TODO: file, line only for RUBY_DEBUG_LOG
void rb_ractor_blocking_threads_dec(rb_ractor_t *r, const char *file, int line); // TODO: file, line only for RUBY_DEBUG_LOG

void rb_ractor_vm_barrier_interrupt_running_thread(rb_ractor_t *r);
void rb_ractor_terminate_interrupt_main_thread(rb_ractor_t *r);
void rb_ractor_terminate_all(void);
bool rb_ractor_main_p_(void);
void rb_ractor_atfork(rb_vm_t *vm, rb_thread_t *th);
void rb_ractor_terminate_atfork(rb_vm_t *vm, rb_ractor_t *th);
VALUE rb_ractor_require(VALUE feature, bool silent);
VALUE rb_ractor_autoload_load(VALUE space, ID id);

VALUE rb_ractor_ensure_shareable(VALUE obj, VALUE name);
st_table *rb_ractor_targeted_hooks(rb_ractor_t *cr);

RUBY_SYMBOL_EXPORT_BEGIN
void rb_ractor_finish_marking(void);

bool rb_ractor_shareable_p_continue(VALUE obj);

// THIS FUNCTION SHOULD NOT CALL WHILE INCREMENTAL MARKING!!
// This function is for T_DATA::free_func
void rb_ractor_local_storage_delkey(rb_ractor_local_key_t key);

RUBY_SYMBOL_EXPORT_END

static inline bool
rb_ractor_main_p(void)
{
    if (ruby_single_main_ractor) {
        return true;
    }
    else {
        return rb_ractor_main_p_();
    }
}

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
rb_ractor_thread_switch(rb_ractor_t *cr, rb_thread_t *th, bool always_reset)
{
    RUBY_DEBUG_LOG("th:%d->%u%s",
                   cr->threads.running_ec ? (int)rb_th_serial(cr->threads.running_ec->thread_ptr) : -1,
                   rb_th_serial(th), cr->threads.running_ec == th->ec ? " (same)" : "");

    if (cr->threads.running_ec != th->ec || always_reset) {
        th->running_time_us = 0;
    }

    if (cr->threads.running_ec != th->ec) {
        if (0) {
            ruby_debug_printf("rb_ractor_thread_switch ec:%p->%p\n",
                              (void *)cr->threads.running_ec, (void *)th->ec);
        }
    }
    else {
        return;
    }

    cr->threads.running_ec = th->ec;

    VM_ASSERT(cr == GET_RACTOR());
}

#define rb_ractor_set_current_ec(cr, ec) rb_ractor_set_current_ec_(cr, ec, __FILE__, __LINE__)
#ifdef RB_THREAD_LOCAL_SPECIFIER
void rb_current_ec_set(rb_execution_context_t *ec);
#endif

static inline void
rb_ractor_set_current_ec_(rb_ractor_t *cr, rb_execution_context_t *ec, const char *file, int line)
{
#ifdef RB_THREAD_LOCAL_SPECIFIER
    rb_current_ec_set(ec);
#else
    native_tls_set(ruby_current_ec_key, ec);
#endif
    RUBY_DEBUG_LOG2(file, line, "ec:%p->%p", (void *)cr->threads.running_ec, (void *)ec);
    VM_ASSERT(ec == NULL || cr->threads.running_ec != ec);
    cr->threads.running_ec = ec;
}

void rb_vm_ractor_blocking_cnt_inc(rb_vm_t *vm, rb_ractor_t *cr, const char *file, int line);
void rb_vm_ractor_blocking_cnt_dec(rb_vm_t *vm, rb_ractor_t *cr, const char *file, int line);

static inline uint32_t
rb_ractor_id(const rb_ractor_t *r)
{
    return r->pub.id;
}

static inline void
rb_ractor_targeted_hooks_incr(rb_ractor_t *cr)
{
    cr->pub.targeted_hooks_cnt++;
}

static inline void
rb_ractor_targeted_hooks_decr(rb_ractor_t *cr)
{
    RUBY_ASSERT(cr->pub.targeted_hooks_cnt > 0);
    cr->pub.targeted_hooks_cnt--;
}

static inline unsigned int
rb_ractor_targeted_hooks_cnt(rb_ractor_t *cr)
{
    return cr->pub.targeted_hooks_cnt;
}

#if RACTOR_CHECK_MODE > 0
# define RACTOR_BELONGING_ID(obj) (*(uint32_t *)(((uintptr_t)(obj)) + rb_gc_obj_slot_size(obj)))

uint32_t rb_ractor_current_id(void);

static inline void
rb_ractor_setup_belonging_to(VALUE obj, uint32_t rid)
{
    RACTOR_BELONGING_ID(obj) = rid;
}

static inline uint32_t
rb_ractor_belonging(VALUE obj)
{
    if (SPECIAL_CONST_P(obj) || RB_OBJ_SHAREABLE_P(obj)) {
        return 0;
    }
    else {
        return RACTOR_BELONGING_ID(obj);
    }
}

extern bool rb_ractor_ignore_belonging_flag;

static inline VALUE
rb_ractor_confirm_belonging(VALUE obj)
{
    if (rb_ractor_ignore_belonging_flag) return obj;

    uint32_t id = rb_ractor_belonging(obj);

    if (id == 0) {
        if (UNLIKELY(!rb_ractor_shareable_p(obj))) {
            rp(obj);
            rb_bug("id == 0 but not shareable");
        }
    }
    else if (UNLIKELY(id != rb_ractor_current_id())) {
        if (rb_ractor_shareable_p(obj)) {
            // ok
        }
        else {
            rp(obj);
            rb_bug("rb_ractor_confirm_belonging object-ractor id:%u, current-ractor id:%u", id, rb_ractor_current_id());
        }
    }
    return obj;
}

static inline void
rb_ractor_ignore_belonging(bool flag)
{
    rb_ractor_ignore_belonging_flag = flag;
}

#else
#define rb_ractor_confirm_belonging(obj) obj
#define rb_ractor_ignore_belonging(flag) (0)
#endif
