# RLGC コードレビュー副読本 (REVIEW_GUIDE.md)

Ractor-Local GC (RLGC) の実装 diff を**コードに即して**読むための副読本。
`git diff de5545202 HEAD`(= master..ractor-local-gc)と**並べて**読むことを想定しています。
箇条書き中心の `RACTOR_LOCAL_GC_DESIGN.md`(設計の経緯)に対し、本書は**変更された実コードを引用し、
何を・なぜ・どう設計に合うか・レビュー時の注意点**を関心ごとに解説します。
現状の到達点・残課題のサマリは `RLGC_STATUS.md` を参照。

ベースコミット `de5545202`(master, RLGC 無し)から **43 コミット**。

## diff 一覧(コード、master..HEAD)

| ファイル | 規模 | 役割 | 本書の § |
|---------|------|------|---------|
| `gc/default/default.c` | ~1200 | RLGC の中核 GC エンジン | §1–4 |
| `gc.c` | ~400 | GC インターフェース層(roots/orphan/keep-alive/id2ref) | §5 |
| `ractor_sync.c` | 90 | メッセージ所有権(materialize-on-receive / in-flight pin) | §6 |
| `variable.c` | 81 | Face B(`rb_const_remove`)+ generic ivar | §7 |
| `ractor.c` | 46 | コピー走査 / foreign EC skip | §6 |
| `vm.c` | 45 | root-fiber / thread-roots マーク | §7 |
| `thread.c` | 23 | Face G/G-2(thread 起動時の re-home) | §7 |
| `symbol.c` | 23 | Face D(symbol set/ids アクセサ + bucket pin) | §7 |
| `ractor_core.h` | 15 | `rb_ractor_sync` の in_flight_materializing | §6 |
| `string.c` | 9 | Face D(fstring table アクセサ) | §7 |
| `iseq.c` | 6 | coverage iseq | §7 |
| `internal/symbol.h` | 2 | アクセサ宣言 | §7 |

計 16 ファイル、+1844 / −154。

## 設計モデル(レビュー前の前提)

- **Ractor ごとに独立した objspace(ヒープ)**。worker のオブジェクトは worker の objspace に住む。
- **ローカル minor GC**: **ロックフリー・VM バリアなし・confined**。マークするのは (a) 自 Ractor の ec/roots、
  (b) shared_bits remset 経由の shareable roots のみで、終わったら **return**。他 objspace のオブジェクトは
  **foreign-skip**(他 objspace のメモリを読まない)。VM-global root は**マークしない**(歴史的に「VM globals は
  main に住む」前提)。
- **グローバル/フル GC**: **STW**(`rb_gc_vm_barrier` で全 Ractor 停止)。全 objspace(orphan 含む)をマーク。
  並行ライタが居ないので**安全に再ルート可**(ガード: `rlgc_global_gc_active` / `rb_gc_during_local_gc_p`)。
- **不変条件**: ① shareable は home objspace で pin され、ローカル GC は**決して shareable を解放しない**
  (sweep guard);② shared_bits = 「shareable から参照される unshareable」の per-page remset、WB が立て
  `gc_mark_shared_roots` がローカル GC でマーク;③ **compaction は RLGC 下で無効**;④ 終了 Ractor は
  **orphan objspace** としてリストに残り、グローバル GC が走査。

## クラッシュ修正(Face)→ コミット対応表

confinement-miss 等の **7 面を修正済**(詳細は各 § と `RLGC_STATUS.md`)。レビュー時はまずこの 7 コミットを
個別に見ると差分が小さく追いやすい:

| Face | 修正概要 | commit | 本書 § |
|------|---------|--------|--------|
| E | `GC.auto_compact` を `!rlgc_has_local` でガード | `95c551e7b` | §4 |
| F | `define/undefine_finalizer` を key 所有者 objspace へルーティング | `72ad765aa` | §4 |
| D | VM-global concurrent_set の if-local keep-alive + symbol bucket pin | `f100f23ba` | §5, §7 |
| B | `rb_const_remove` の lookup+削除を VM ロックで atomic 化 | `808e41fd9` | §7 |
| G | thread 割り込み queue/mask-stack を子 objspace へ re-home | `f8885699f` | §7 |
| G-2 | fiber-storage Hash を re-home | `c0e1c99fe` | §7 |
| trap | signal-trap handler を `trap_list.cmd[]` if-local keep-alive | `51819fc7b` | §5, §7 |

元の RLGC 実装本体は `14047e008`「per-Ractor objspace + lock-free local GC」以降の一連のコミット。

## 推奨レビュー順

1. **§1**(objspace ライフサイクル・ロックフリー確保)で「per-Ractor objspace + span」のメンタルモデルを掴む
2. **§2**(confined mark / sweep guard)で「foreign-skip と shareable pin」= 安全性の要を見る
3. **§3**(shared_bits / WB / remset)で「cross-objspace 参照をどう生かすか」を見る
4. **§4**(global GC / compaction guard / finalizer)で STW 経路と Face E/F
5. **§5**(`gc.c` interface)で roots / orphan / keep-alive(Face D/trap)/ id2ref
6. **§6**(メッセージ所有権)で materialize-on-receive と in-flight pin
7. **§7**(satellite)で Face B/G/G-2/D の各サブシステム修正

---
## 1. Per-Ractor objspace lifecycle & lock-free allocation

This section covers `gc/default/default.c`: how each Ractor gets its own objspace, how that objspace allocates heap pages lock-free out of per-objspace mmap arenas, how a VM-global memory span lets any thread resolve a heap pointer to its owning objspace without a lock, the one-time process-globals init guard, and how `gc_enter` chooses the no-lock local path vs the STW global path. The whole subsystem is gated by `RACTOR_LOCAL_GC`, defaulted to `1` (default.c:268-270).

### 1.1 The two new objspace flavors: `local`, plus `local_gc` / `global_gc`

default.c:553-557 (and the flags bitfield 570-580)
```c
#if RACTOR_LOCAL_GC
    /* true for a per-Ractor (non-main) objspace: collected locally without a STW barrier. */
    bool local;
#endif
```
```c
        unsigned int local_gc : 1;   /* set while THIS objspace is being collected in local mode */
        unsigned int global_gc : 1;  /* set while a GLOBAL all-Ractor STW GC is in progress */
```

**What:** `objspace->local` is the *static* property "this objspace belongs to a non-main Ractor"; `flags.local_gc` / `flags.global_gc` are *transient*, set only for the duration of one collection. **Why:** these three booleans are the master switches the rest of the file keys off (`objspace->local || rlgc_has_local`, `!rlgc_global_gc_active`, etc.). **How it fits:** `local` decides whether a collection may run confined and lock-free; `global_gc` overrides that to force the STW unified path even on a local objspace. **Gotcha:** note the asymmetry between `objspace->local` (per-objspace) and the file-static `rlgc_has_local` (process-wide "at least one local objspace exists"). Reviewers should check each use-site: the main objspace has `local == false` but, once any worker exists, must *also* behave RLGC-aware (hence the recurring `objspace->local || rlgc_has_local`).

### 1.2 Where a Ractor objspace is born: the `objspace_init` fork

`rb_gc_impl_objspace_alloc` itself is unchanged (default.c:10653, still just `calloc1(sizeof(rb_objspace_t))`). All per-Ractor logic lives in `rb_gc_impl_objspace_init`, which runs once per objspace (default.c:10665-10687):
```c
    if (rlgc_main_objspace == NULL) {
        rlgc_main_objspace = objspace;            /* first objspace == the main Ractor's */
    }
    else {
        objspace->local = TRUE;
        objspace->flags.dont_incremental = TRUE;  /* local GC = single self-contained STW-of-one */
        if (!rlgc_has_local) {
            /* First worker: main GCs must also become atomic so the global barrier never
             * catches main mid-incremental-mark / lazy-sweep. */
            gc_rest(rlgc_main_objspace);
            rlgc_main_objspace->flags.dont_incremental = TRUE;
        }
        rlgc_has_local = true;
    }
```

**What:** the first objspace ever initialized is recorded as `rlgc_main_objspace`; every subsequent one is flagged `local`. **Why `dont_incremental`:** a local GC must leave *no* GC state alive across mutator or sibling-Ractor execution (it runs with no barrier), so it is forced to immediate (non-incremental) mark and immediate sweep — `dont_incremental` implies immediate sweep via `gc_start`. **Why finish main's in-flight GC:** once global GC is possible, its STW barrier must never freeze the main objspace mid-incremental-collection (the unified mark/sweep cannot resume someone else's partial state), so main is also downgraded to atomic, and any pending main collection is flushed with `gc_rest`. **Gotcha:** this transition is only safe because of the *stated* assumption "we are on the main Ractor here (the first non-main Ractor is always created by main) with no other Ractor running." If that VM invariant ever changes (a worker spawning the very first sibling), the unguarded `gc_rest(rlgc_main_objspace)` on a foreign objspace becomes a data race. Worth a reviewer flag.

### 1.3 The VM-global heap span: lock-free pointer → objspace resolution

default.c (diff lines 189-247). The file-statics:
```c
static rb_objspace_t *rlgc_main_objspace = NULL;
static bool rlgc_has_local = false;
static bool rlgc_global_gc_active = false;
static size_t rlgc_global_lomem = 0;   /* grow-only VM-global heap span */
static size_t rlgc_global_himem = 0;
```
The grow-only updater and the membership test:
```c
static inline void
rlgc_span_extend(uintptr_t lo, uintptr_t hi)
{
    size_t cur;
    while ((cur = rlgc_span_load(&rlgc_global_lomem)) == 0 || lo < cur) {
        if (RUBY_ATOMIC_SIZE_CAS(rlgc_global_lomem, cur, (size_t)lo) == cur) break;
    }
    while ((cur = rlgc_span_load(&rlgc_global_himem)) < hi) {
        if (RUBY_ATOMIC_SIZE_CAS(rlgc_global_himem, cur, (size_t)hi) == cur) break;
    }
}
```
```c
static inline bool
rlgc_obj_in_any_heap(VALUE obj)
{
    const uintptr_t p = (uintptr_t)obj;
    if (p % sizeof(VALUE) != 0) return false;
    const uintptr_t body = (uintptr_t)GET_PAGE_BODY(p);
    if (body < rlgc_span_load(&rlgc_global_lomem) || p >= rlgc_span_load(&rlgc_global_himem)) return false;
    struct heap_page *const page = GET_HEAP_PAGE(p);
    return page != NULL && (uintptr_t)page->body == body; /* page back-pointer round-trips */
}
```

**What:** `[lomem, himem)` is a single union bounding-box over *every* objspace's page bodies. Any thread can range-test a pointer against it, then mask down to the aligned page body (`GET_PAGE_BODY`) and read `page->objspace` (the new back-pointer, §1.4). **Why:** the alternative — a per-objspace sorted page array (`is_pointer_to_heap`) — only knows *one* objspace's pages and needs that objspace's lock; the span knows *all* of them with two relaxed atomic loads, which is what makes cross-objspace reference recognition (e.g. `check_rvalue_consistency_force` at default.c diff lines ~1670) lock-free. **How it's kept correct under concurrency:** producers are `rlgc_span_extend` (called once per arena grow, §1.5, and once per `heap_page_allocate`, default.c:2441 area), using CAS loops so concurrent Ractors growing their arenas never lose an update. The span is grow-only (`lomem` only shrinks, `himem` only grows), so a stale relaxed read can only *fail to include a just-added page, never exclude an existing one* — and during the STW global GC, where the span actually gates marking, it is stable. **Gotcha:** `rlgc_obj_in_any_heap` is documented as safe only for *real* object pointers — it dereferences the page body (`page->body`). It is NOT a conservative scanner; for arbitrary words the code uses `rb_gc_conservative_owner` (declared at the diff's top, implemented in gc.c). Also note `lomem` uses the page *body* base, not the page `start`, because a masked-down pointer can legitimately sit below `start` (comment at the `rlgc_span_extend` call in `heap_page_allocate`). A reviewer should confirm the span is never *read* expecting precision finer than page-alignment.

### 1.4 Page → objspace back-pointer and the per-page shared remset

default.c (diff lines ~888-910):
```c
#if RACTOR_LOCAL_GC
    /* Owning objspace. ... lets a local GC tell its own objects from objects living in
     * another Ractor's (or the main) objspace. */
    rb_objspace_t *objspace;
#endif
    ...
    bits_t shared_bits[HEAP_PAGE_BITMAP_LIMIT];   /* boundary remset */
```
And the page-flag widening from bitfield to full `unsigned char` (diff ~875-887):
```c
    unsigned char has_remembered_objects;
    unsigned char has_uncollectible_wb_unprotected_objects;
    unsigned char has_shared_objects;  /* RLGC only */
```

**What:** every page gains `objspace` (set in `heap_page_allocate`, default.c:2455 area: `page->objspace = objspace;`), plus a `shared_bits` boundary remset and a `has_shared_objects` summary flag. **Why the bitfield→byte change:** these flags are now written by the *lock-free write barrier* from arbitrary Ractor threads. A bitfield `flags.has_x = TRUE` compiles to a read-modify-write of the whole word, so two Ractors setting sibling flags concurrently lose one update; promoting each to its own byte makes the store atomic-by-isolation. The same reasoning drives `MARK_IN_BITMAP_ATOMIC` / `gc_bitmap_atomic_set` (diff ~1010-1040) for `shared_bits`/`remembered_bits`, which share one `bits_t` word across many slots and so need a real CAS. **How it fits §1:** `page->objspace` is the payload the span lookup (§1.3) returns; it is the mechanism by which a confined local GC "foreign-skips" objects it doesn't own. **Gotcha:** this is squarely in the territory of the MEMORY note's TSan-confirmed bitmap RMW bug — reviewers should verify that *every* multi-Ractor-written page bitmap/flag goes through the atomic path, not a plain `|=` or bitfield assignment.

### 1.5 Per-objspace arena allocator (the parallelism unlock)

default.c (diff lines ~602-614 for the fields; ~2120-2200 for the allocator). The objspace gains:
```c
struct rlgc_page_arena *arenas;        /* mmap'd arenas, munmap at objspace free */
char *arena_cursor; char *arena_end;   /* bump pointer */
struct heap_page_body *arena_freelist; /* recycled bodies (link stored in the body) */
```
Reserve one big arena per ~256 bodies:
```c
#define RLGC_PAGE_ARENA_BODIES 256
#define RLGC_ARENA_ALIGN (2u * 1024 * 1024)
...
    char *const ptr = mmap(NULL, mmap_size, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
    ...
    char *aligned = ptr + RLGC_ARENA_ALIGN;
    aligned -= ((uintptr_t)aligned & (RLGC_ARENA_ALIGN - 1));
#ifdef MADV_HUGEPAGE
    madvise(aligned, arena_size, MADV_HUGEPAGE);
#endif
    ...
    rlgc_span_extend((uintptr_t)aligned, (uintptr_t)aligned + arena_size);
```
Carving a body (in `heap_page_body_allocate`, now taking `objspace`, diff ~2311-2340):
```c
    if (objspace->heap_pages.arena_freelist != NULL) {
        page_body = objspace->heap_pages.arena_freelist;
        objspace->heap_pages.arena_freelist = *(struct heap_page_body **)page_body;
    }
    else {
        if (objspace->heap_pages.arena_cursor + HEAP_PAGE_SIZE > objspace->heap_pages.arena_end) {
            if (!rlgc_page_arena_grow(objspace)) return NULL;
        }
        page_body = (struct heap_page_body *)objspace->heap_pages.arena_cursor;
        objspace->heap_pages.arena_cursor += HEAP_PAGE_SIZE;
    }
```
Freeing recycles instead of `munmap` (`heap_page_body_free`, now taking `objspace`, diff ~2188-2200):
```c
    asan_unpoison_memory_region(page_body, sizeof(struct heap_page_body *), false);
    *(struct heap_page_body **)page_body = objspace->heap_pages.arena_freelist;
    objspace->heap_pages.arena_freelist = page_body;
```

**What:** instead of one `mmap`+two `munmap` per 64 KiB page, each objspace reserves ~16 MiB arenas and bump-allocates/recycles bodies entirely within its own private state. **Why:** the documented root cause — per-page `mmap`/`munmap` for 64 KiB alignment serialized every Ractor on the kernel's process-wide `mmap_lock`, and the anonymous first-touch fault rate (not bandwidth) was the parallelism ceiling. The 2 MiB-aligned, `MADV_HUGEPAGE` arena first-touches in one fault per 2 MiB (512× fewer), and the alignment slack is deliberately *not* `munmap`'d (each `munmap` would re-take `mmap_lock` for write + a TLB shootdown). **How it fits:** arenas are per-objspace, so the page-grab fast path needs no lock — the prerequisite for lock-free allocation (§1.7); each grow extends the VM-global span (§1.3) so the new bodies become resolvable. **Gotchas:** (1) the freelist link lives *inside* the freed body, which may be ASAN-poisoned post-sweep — note the explicit `asan_unpoison_memory_region` before writing the link; dropping that would be an ASAN false-positive crash. (2) Both `heap_page_body_allocate` and `heap_page_body_free` now require the correct owning `objspace` — passing the wrong one would cross-link freelists between Ractors and corrupt two heaps. Every call site was updated (`heap_page_free`, `heap_page_allocate`); reviewers should grep for any remaining zero-arg caller. (3) Physical memory for a worker's pages is only returned to the OS when the whole objspace is freed (`rlgc_page_arenas_free`, §1.6) — a churny-then-idle Ractor holds its arenas until termination.

### 1.6 Objspace teardown and orphan tolerance

`rb_gc_impl_objspace_free` (diff ~10478-10500):
```c
    if (objspace == rlgc_main_objspace && getenv("RLGC_STATS")) { fprintf(...); }
    ...
#if RACTOR_LOCAL_GC && defined(HAVE_MMAP)
    if (HEAP_PAGE_ALLOC_USE_MMAP) rlgc_page_arenas_free(objspace); /* munmap the page arenas */
#endif
```
`rlgc_page_arenas_free` (diff ~363-374) walks the arena list, `munmap`s each `mmap_base`, and nulls the cursors. The orphan-tolerance fix in `heap_pages_free_unused_pages` (diff ~2257-2278):
```c
    /* An objspace can legitimately have ZERO pages here under Ractor-local GC: an orphaned
     * (terminated-Ractor) objspace whose last live object has been reclaimed ... rb_darray_get(
     * sorted, -1) would then read out of bounds ... Only recompute bounds when pages remain. */
    if (rb_darray_size(objspace->heap_pages.sorted) > 0) {
        ... heap_pages_himem = ...; heap_pages_lomem = ...;
    }
```

**What:** objspace free now releases the arenas, and the unused-page reclaimer no longer assumes ≥1 page. **Why:** a terminated Ractor leaves an *orphaned* objspace on a list the global GC still walks; the global GC can reclaim its last live shareable, leaving an empty `sorted` array. The pre-RLGC code unconditionally did `rb_darray_get(sorted, size-1)` — `rb_darray_get(sorted, -1)` reads out of bounds and dereferences a NULL page. **How it fits:** this is the allocation-layer half of the orphaned-objspace story that dominates the MEMORY notes (the mark-side half is the orphan-list walk in gc.c). **Gotcha:** the comment explicitly defers *freeing the empty orphan shell* to a follow-up (RACTOR_LOCAL_GC_DESIGN.md 5.4) — so an idle process accumulates empty orphan objspace structs. Reviewers tracking the open "deep-graph mark-T_NONE" crash should note this is the exact orphan×reclaimed-page interplay region.

### 1.7 Lock-free allocation path selection in `newobj_cache_miss`

default.c:2914 (diff ~2892-2917):
```c
    if (!vm_locked && !(objspace->local && rlgc_lockfree_alloc_enabled())) {
        lev = RB_GC_CR_LOCK();
        unlock_vm = true;
    }
```
`rlgc_lockfree_alloc_enabled()` (diff ~265-285) is `RUBY_RACTOR_LOCAL_GC_LOCKFREE` (default ON; only `"0"` disables).

**What:** for a local objspace with lock-free alloc enabled, the cache-miss page grab takes **no** VM lock. **Why:** the single `vm->ractor.sync.lock` taken on *every* newobj cache miss was the dominant scaling bottleneck — it serialized all Ractors' allocation. The page grab now touches only this Ractor's private heap/freelist (§1.5), so it parallelizes. **How it fits the design:** the second comment block (the "fully lock-free" one that supersedes the first) is the load-bearing correctness statement — even the *local GC* triggered by a refill runs without the VM lock, which is only sound because every VM-global structure such a GC touches (`generic_fields_tbl`, `id2ref`, finalizer/symbol tables, Ractor ports) is independently made Ractor-GC-safe via *non-barrier* locks shared with its mutators. **Gotchas:** (1) two comment blocks are present — the first describes an earlier "GC still takes the barrier-aware lock" design and is now stale/misleading; the actual code matches the *second* block (fully lock-free incl. the local GC). A reviewer should treat the first block as historical and consider deleting it. (2) The main objspace (`local == false`) always locks — correct, since many Ractors allocate into it. (3) `newobj_slowpath` (diff ~2963-2975) was deliberately *not* made lock-free — `lev` is now initialized to `0` and it always takes `RB_GC_CR_LOCK()`, because the `during_gc`/stress slow path is rare and needs exclusive access. Confirm no path reaches the lock-free fast allocation while `during_gc` is set on that objspace.

### 1.8 Ractor newobj-cache flush routing (ownership correctness)

`gc_ractor_newobj_cache_clear` now takes the target objspace via `data` instead of `rb_gc_get_objspace()` (diff ~4323-4340):
```c
static void
gc_ractor_newobj_cache_clear(void *c, void *data)
{
    rb_objspace_t *objspace = (rb_objspace_t *)data;   /* was: rb_gc_get_objspace() */
    ...
```
And `gc_sweep_start` routes which caches to flush (default.c:4385-4400):
```c
    if (rlgc_global_gc_active) {
        rb_gc_ractor_newobj_cache_foreach_for_objspace(objspace, gc_ractor_newobj_cache_clear, objspace);
    }
    else if (objspace->local) {
        rb_gc_ractor_newobj_current_cache_foreach(gc_ractor_newobj_cache_clear, objspace);
    }
    else {
        rb_gc_ractor_newobj_cache_foreach(gc_ractor_newobj_cache_clear, objspace);
    }
```

**What:** a cache is always flushed into *the objspace it allocates into*. Global GC flushes only the caches belonging to the objspace currently being swept; a local GC flushes only its own Ractor's current cache; the legacy single-objspace path flushes all into the one objspace. **Why:** a newobj cache's freelist holds slots living in *its own* objspace; appending it to a foreign heap corrupts both. The old `gc_ractor_newobj_cache_clear` hardcoded `rb_gc_get_objspace()`, which is wrong once caches and objspaces are 1:N. **How it fits:** mirrors the same ownership discipline as the arena freelist (§1.5) — slots never cross objspace boundaries. **Gotcha:** the fix touches four call sites — `gc_sweep_start`, `rb_gc_impl_ractor_cache_free` (diff ~1426: now passes `objspace`, not `NULL`), and `rb_gc_impl_after_fork` (diff ~10580-10595, which under `rlgc_has_local` iterates *every* objspace via `rb_gc_foreach_objspace` rather than dumping all caches into main). A reviewer should confirm no remaining caller passes `NULL` as `data`, which would now dereference NULL rather than silently using the current objspace.

### 1.9 `gc_enter` / `gc_exit`: local (no-lock) vs global (STW) selection

`gc_enter` (default.c:7700-7740, diff ~7697-7745):
```c
    if (objspace->local && !objspace->flags.global_gc) {
        /* Ractor-local MINOR GC: ... Do NOT take the VM lock and do NOT stop other Ractors. */
        *lock_lev = 0;
        objspace->flags.local_gc = TRUE;
        ... rlgc_concurrent_local_gc++ / max tracking ...
    }
    else {
        *lock_lev = RB_GC_VM_LOCK();
        switch (event) { case ...start/rest/continue: rb_gc_vm_barrier(); ... }
        /* GLOBAL or main GC: UNCONFINED unified mark across all (barrier-stopped) objspaces. */
    }
```
`gc_exit` mirrors it (default.c:7755-7773): the local branch only clears `local_gc` and decrements the concurrency counter; the global branch clears both flags and calls `RB_GC_VM_UNLOCK(*lock_lev)`.

**What:** the single decision point that splits collections into (a) confined local minor GC — no `RB_GC_VM_LOCK`, no `rb_gc_vm_barrier`, `lock_lev = 0`; and (b) everything else — full VM lock + barrier (all Ractors stopped) for global/full GC and any main-objspace GC. **Why:** the local path is what lets N Ractors mark+sweep their own heaps simultaneously; the `rlgc_concurrent_local_gc` / `rlgc_max_concurrent_local_gc` counters (printed at shutdown via `RLGC_STATS`) exist precisely to *prove* overlap (max > 1 means real wall-clock concurrency). **How it fits:** `global_gc` is predicted in `gc_start` (diff ~7384-7407) *before* `gc_enter`, because the barrier decision must be known up front — a full/major collection (`will_full_mark`) is promoted to a global STW GC; minor stays local. `gc_start` then drives `gc_global_sweep` vs `gc_sweep` and toggles `rlgc_global_gc_active` around the unified mark. **Gotchas:** (1) `lock_lev = 0` on the local path means `gc_exit` must *not* call `RB_GC_VM_UNLOCK(0)` — verify the branch symmetry holds (it does: local exit skips the unlock). (2) The local path's `local_gc`/`global_gc` flag handshake must be exception-safe — if a local GC could longjmp out between `gc_enter` and `gc_exit`, the flags and the concurrency counter would leak; reviewers should confirm the collection body cannot escape non-locally. (3) The "main objspace GC runs unconfined" rule depends on §1.2 having forced main to `dont_incremental`; if main ever ran incrementally again, the global barrier could catch it mid-mark.

### 1.10 One-time process-globals init guard

`rb_gc_impl_objspace_init`, after the per-Ractor branch (diff ~10713-10735):
```c
    {
        static bool process_globals_initialized = false;
        if (!process_globals_initialized) {
            init_size_to_heap_idx();
#if defined(INIT_HEAP_PAGE_ALLOC_USE_MMAP)
            heap_page_alloc_use_mmap = INIT_HEAP_PAGE_ALLOC_USE_MMAP;
#endif
            process_globals_initialized = true;
        }
    }
```
and `gc_params.heap_init_bytes = GC_HEAP_INIT_BYTES;` was *removed* from this function entirely.

**What:** the objspace-independent, process-global initializers (`init_size_to_heap_idx`, the runtime `heap_page_alloc_use_mmap` probe) now run exactly once instead of on every objspace init. **Why:** `objspace_init` runs per Ractor, but these write *process globals* that other Ractors already read lock-free; re-writing them on each child-Ractor init races those readers, and re-setting `heap_init_bytes` would clobber a `RUBY_GC_HEAP_INIT_BYTES`-tuned value. The first call is the main objspace at boot (no other Ractor running) and later calls hold the VM lock, so a plain `static bool` guard publishes with clean happens-before. `heap_init_bytes` is dropped here because its static initializer + `rb_gc_impl_set_params` already set it. **How it fits:** complements §1.7/§1.3 — anything a worker reads lock-free must not be concurrently re-initialized. **Gotcha:** the `static bool` is unsynchronized; its safety rests entirely on the stated "first call is single-threaded at boot" assumption. If a worker Ractor could ever be the *first* to call `objspace_init` (same fragility as §1.2), the guard races. Reviewers should treat §1.2 and §1.10 as sharing one load-bearing precondition: the main Ractor's objspace is always initialized first, alone.

---

## 2. Confined local mark & sweep guards (`gc/default/default.c`)

These are the load-bearing guards that make a confined local GC *safe*: how it marks (skip foreign objects, treat their refs as live leaves), how it ages/pins shareables instead of mutating them, how it sweeps (the absolute rule "a local GC must NEVER free a shareable"), and how the STW global GC lifts those guards to reclaim genuinely-dead shareables. Read this alongside Section 1 (the flags/bitmaps/RLGC scaffolding) — `objspace->flags.local_gc`, `rlgc_global_gc_active`, `rlgc_has_local`, `shared_bits`/`GET_HEAP_SHARED_BITS`, and `GET_HEAP_OBJSPACE` are all introduced there.

### 2.1 `gc_mark`: the foreign-skip (confinement)

`gc/default/default.c:5157` (function at `:5152`)

```c
#if RACTOR_LOCAL_GC
    /* Confined local GC: skip objects owned by another objspace ... */
    if (objspace->flags.local_gc && GET_HEAP_OBJSPACE(obj) != objspace) {
        return;
    }
#endif

    rgengc_check_relation(objspace, obj);
#if RACTOR_LOCAL_GC
    gc_shared_relation(objspace, obj);
#endif
    if (!gc_mark_set(objspace, obj)) return; /* already marked */
```

**What:** During a per-Ractor local GC (`flags.local_gc`), if the reached object lives in *another* objspace (`page->objspace != objspace`), `gc_mark` returns immediately — it does not mark it, does not set its mark bit, does not grey it (so its children are never traversed). The reference is effectively a live leaf.

**Why:** This is the core of "CONFINED" marking. A local GC runs lock-free with no VM barrier, so other Ractors (incl. the owner of `obj`) are concurrently mutating their own heaps. Reading or writing a foreign object's mark bit / flags / children would race those mutators. The foreign object stays alive via *its own* objspace's roots; this GC has no business deciding its liveness. The check is by alignment via `GET_HEAP_OBJSPACE` (the `page->objspace` back-pointer from Section 1), so it is O(1) and never dereferences `obj`'s contents.

**How it fits:** This is what lets a minor GC "RETURN after marking only its own ec/roots + shared_bits roots." The exception path for genuinely-cross-objspace-but-still-needs-marking objects is the *global* GC, where `flags.local_gc` is false, so this branch is skipped and the unified mark traverses everything.

**Reviewer gotchas:**
- The skip is gated on `flags.local_gc`, not `objspace->local`. During a *global* GC the driver objspace is `local` but `local_gc` is **false** (see `gc_enter` in Section 1), so the global mark correctly does *not* foreign-skip — that's deliberate and essential, otherwise the unified mark would stop at the first cross-objspace edge.
- `GET_HEAP_OBJSPACE(obj)` is valid only for a real heap object. `gc_mark` is only ever reached for verified references, so this is fine here — but note the contrast with `rb_gc_impl_mark_maybe` (conservative scan), which must use the safe `rb_gc_conservative_owner` / `rlgc_obj_in_any_heap` instead.

### 2.2 `gc_shared_relation`: rebuilding `shared_bits` ground truth during the (global) mark

`gc/default/default.c:5038`, called from `gc_mark` at `:5169`

```c
static inline void
gc_shared_relation(rb_objspace_t *objspace, VALUE obj)
{
    VALUE parent = objspace->rgengc.parent_object;
    if (!SPECIAL_CONST_P(parent) &&
        RB_OBJ_SHAREABLE_P(parent) &&
        !RB_OBJ_SHAREABLE_P(obj)) {
#if RACTOR_LOCAL_GC_AUDIT
        if (!MARKED_IN_BITMAP(GET_HEAP_SHARED_BITS(obj), obj)) {
            gc_shared_wb_miss(objspace, parent, obj);
        }
#endif
        MARK_IN_BITMAP_ATOMIC(GET_HEAP_SHARED_BITS(obj), obj);
        GET_HEAP_PAGE(obj)->flags.has_shared_objects = TRUE;
    }
}
```

**What:** Called on *every* marked edge (`parent` = `rgengc.parent_object`, `obj` = child). When a **shareable parent** directly references an **unshareable child**, that child is a shareable→unshareable *boundary* object; `gc_shared_relation` records it in `shared_bits` and flags the page `has_shared_objects`.

**Why:** `shared_bits` is the remset of unshareable objects kept alive only because a shareable references them. Between collections the *write barrier* (`rb_gc_impl_writebarrier`, Section 3) maintains it. But the WB can have gaps, and the set must be authoritative. So a **full mark rebuilds `shared_bits` from scratch as it walks** — the mark itself is the ground truth. `shared_bits` is cleared at `gc_marks_start` (only when all shareable parents are visible — single-objspace, or a global STW GC; see 2.6), and `gc_shared_relation` repopulates it.

**How it fits:** This closes the loop with `gc_mark_shared_roots` (Section 1 / 2.6): the *next* minor GC reads the `shared_bits` this mark wrote, to root boundary objects whose shareable parent lives in another objspace it cannot see.

**Reviewer gotchas:**
- The set is `MARK_IN_BITMAP_ATOMIC` because `shared_bits` is written concurrently by the lock-free WB and by overlapping local GCs; a non-atomic `|=` would lose a sibling object's bit in the same word (this is the ThreadSanitizer-confirmed bitmap-RMW class of bug). Don't "optimize" it back to `MARK_IN_BITMAP`.
- `RACTOR_LOCAL_GC_AUDIT` (off by default) turns this into a **WB-completeness checker**: if the mark finds a boundary edge the WB had *not* already recorded, `gc_shared_wb_miss` (`:5021`) reports it — that's how WB-miss bugs (Family I/III) are hunted.
- It keys on `rgengc.parent_object`, which must be set correctly along the mark path; `gc_mark_set_parent_invalid`/`_raw` bracket the root passes.

### 2.3 `gc_aging`: shareables are pinned, never aged in place

`gc/default/default.c:5076`

```c
#if RACTOR_LOCAL_GC
    /* A shareable object is concurrently read by other Ractors' mutators ...
       A lock-free per-Ractor LOCAL GC must NOT read-modify-write its flags word ... */
    if (objspace->flags.local_gc && RB_OBJ_SHAREABLE_P(obj)) {
        objspace->marked_slots++;
        return;
    }
#endif
```

**What:** In a local GC, when marking reaches a shareable object, `gc_aging` increments `marked_slots` (so accounting stays consistent — it *is* live) and returns *before* touching the object's age/flags.

**Why:** Aging does `RVALUE_AGE_INC` / `RVALUE_OLD_UNCOLLECTIBLE_SET` / `FL_PROMOTED`, i.e. a **read-modify-write of the object's flags word**. A shareable's flags are concurrently read by other Ractors (e.g. `vm_ic_hit_p`) and its generational state belongs to the STW global GC. A lock-free RMW here would tear the flags word. So generational state of shareables is owned exclusively by the global GC.

**How it fits:** Consistent with the whole "shareables are pinned in their home objspace during a local GC" invariant — they are marked-as-live but otherwise left untouched. The early `marked_slots++` mirrors the function's normal tail (`:5132`) so per-objspace slot accounting doesn't drift.

**Reviewer gotcha:** Again gated on `flags.local_gc`, not `local` — under a global GC, shareables *are* aged normally (the STW global GC is the one place that may). Note this is the mark-time counterpart to the *sweep-time* shareable pin in 2.4; both are needed.

### 2.4 The CRITICAL sweep guard: a local GC must NEVER free a shareable

`gc/default/default.c:4057` (inside `gc_sweep_plane`, function at `:4023`)

```c
#if RACTOR_LOCAL_GC
    if ((objspace->local || rlgc_has_local) && RB_OBJ_SHAREABLE_P(vp) && !rlgc_global_gc_active) {
        /* A NON-global GC cannot tell whether a shareable object is still referenced
           from another objspace ... so it must never free shareables — they stay
           pinned until a GLOBAL GC. ... */
        break;
    }
    if (rlgc_has_local && !rlgc_global_gc_active && RB_OBJ_SHAREABLE_P(vp) &&
        BUILTIN_TYPE(vp) == T_IMEMO &&
        (imemo_type(vp) == imemo_callcache || imemo_type(vp) == imemo_callinfo ||
         imemo_type(vp) == imemo_ment)) {
        /* cc / ci / cme imemo: shared VM infra reached cross-Ractor via WEAK inline
           caches a confined GC cannot trace ... so keep them pinned too ...
           but the GLOBAL GC LIFTS the guard. */
        break;
    }
#endif
```

(The `break` exits the per-slot `switch` to the next slot — i.e. *do not* fall through to the free path.)

**What:** In any non-global sweep, an unmarked shareable object is **skipped, not freed**. Two cases: (a) all shareables; (b) belt-and-suspenders for the three VM-infra imemo types (callcache / callinfo / method-entry) which are shareable and reached through weak inline caches.

**Why:** This is RLGC invariant #3 made operational. A confined GC has no way to know a shareable is dead — it may be referenced from a class's cc-table, a shape edge table, an inline cache, the Ractor object, or another Ractor entirely, none of which a local mark traversed. Freeing it would dangle those cross-objspace references *and* its own unshareable children (which `shared_bits` was keeping alive). So shareables are pinned for the objspace's lifetime and only the global GC reclaims them.

**The widened condition `(objspace->local || rlgc_has_local)` is the subtle, important part.** It is not enough to pin shareables only in *worker* (`local`) objspaces. The **main** objspace runs ordinary minor/compaction GCs that are *not* global once any worker exists (`rlgc_has_local`). A shareable in main can be live *only* via a worker (a Ractor body's isolated env, a sent shareable graph) that main's own roots don't reach. If main's minor GC freed it, that surfaced as a worker UAF / cross-objspace mark-T_NONE. So the pin must also apply to the main objspace's non-global GCs — hence `rlgc_has_local`.

**`!rlgc_global_gc_active` lifts the guard for the global GC.** The STW unified mark *does* establish true cross-objspace reachability, so there an unmarked shareable is genuinely dead and **must** be reclaimed together with its now-dead subtree. Critically, the cc/ci/cme imemo pin is *also* lifted globally and **must** be: classes are shareable and a dead class is collected by the global GC; a cc/cme pinned through the global GC would survive holding a strong owner/def reference to that freed class → dangling (this is the cc_tbl UAF family).

**How it fits:** Pairs with `gc_aging`'s mark-time shareable skip (2.3): shareables are kept-alive-and-untouched at mark, and never-freed at sweep, for any non-global collection.

**Reviewer gotchas:**
- The whole guard is `!rlgc_global_gc_active`. If you ever change global-GC entry such that the flag isn't set during `gc_global_sweep_one`, you'd silently leak every dead shareable VM-wide. The flag is set in `gc_start` around the mark+sweep and cleared after (Section 1).
- The first clause already matches *all* shareables, so the second cc/ci/cme clause is **only reachable when the first does not fire** — i.e. when `objspace->local` is false *and* `rlgc_has_local` is false, which under RLGC defaults can't happen for a shareable... in practice it's defensive/audit redundancy. Worth a reviewer note: confirm it's intended as belt-and-suspenders, not dead code masking a logic gap. The distinct, more-specific comment (weak ICs, cross-objspace remember-bit races) documents *why* these three types are the dangerous ones.
- The condition reads `vp`'s type/imemo_type — safe because the slot's flags are still intact at this point in the sweep (the object hasn't been zeroed).

### 2.5 The audit backstop: `rb_bug` if a non-global GC reaches the free path with a shareable

`gc/default/default.c:4115` (a few lines after the pin, before the actual free)

```c
#if RACTOR_LOCAL_GC_AUDIT
    /* u->s liveness invariant: a per-Ractor local GC must NEVER free a shareable object ... */
    if ((objspace->local || rlgc_has_local) && !rlgc_global_gc_active && RB_OBJ_SHAREABLE_P(vp)) {
        rb_bug("RLGC-AUDIT: non-global GC freeing a shareable object: %s", rb_obj_info(vp));
    }
#endif
```

**What / why:** Under `RACTOR_LOCAL_GC_AUDIT`, this asserts the 2.4 invariant *constructively*: if execution ever reaches the free path for a shareable in a non-global GC, the pin above was bypassed and we `rb_bug` immediately with object info — instead of silently freeing it and crashing far away later (a dangling unshareable child, hard to root-cause). It uses the exact same predicate as the pin, so it can only fire if the pin's logic and this check diverge, i.e. a real bug.

**Reviewer gotcha:** This is *audit-build only* (off by default), so it does not protect production builds — it's a development tripwire. The same predicate appearing twice is intentional (guard vs. assertion of the guard); if you edit one, edit both.

### 2.6 `gc_marks_start` / `gc_marks_finish`: local-vs-global mark behaviour

`gc_marks_start` — `gc/default/default.c:6615`; the clear was factored into `gc_full_mark_clear_objspace` (`:6585`):

```c
#if RACTOR_LOCAL_GC
        if (rlgc_global_gc_active) {
            /* GLOBAL GC: the unified mark below repopulates mark/old/remembered/shared
               bits and counters across EVERY objspace, so clear them everywhere first. */
            rb_gc_foreach_objspace(gc_full_mark_clear_thunk, NULL);
        }
        else
#endif
        {
            gc_full_mark_clear_objspace(objspace);
        }
```

**What:** The full-mark reset (zero counters; `rgengc_mark_and_rememberset_clear`; move pooled→free pages) was extracted into `gc_full_mark_clear_objspace` so a global GC can apply it to **every** objspace (via `rb_gc_foreach_objspace`) before the single unified mark repopulates them all; a local/single full mark clears only its own objspace.

**Why:** A global GC clears *and re-marks* all objspaces as one graph, so all their bitmaps must be reset up front. The shareable-related part of the clear is the subtle bit — see `rgengc_mark_and_rememberset_clear` (`:6940` region):

```c
#if RACTOR_LOCAL_GC && !RACTOR_LOCAL_GC_AUDIT
    /* shared_bits ... may only be cleared+recomputed when ALL shareable parents are visible:
       either the pure single-objspace case, or a GLOBAL all-objspace GC. ...
       A per-Ractor LOCAL GC must NOT clear it (it cannot see foreign parents) ... */
    if (!rlgc_has_local || rlgc_global_gc_active) {
        memset(&page->shared_bits[0], 0, HEAP_PAGE_BITMAP_SIZE);
        page->flags.has_shared_objects = FALSE;
    }
#endif
```

A local GC clearing `shared_bits` would be catastrophic: it can't see the foreign shareable parents that justify those bits, so `gc_shared_relation` couldn't rebuild them, and the boundary objects would be swept. So `shared_bits` is only ever cleared+rebuilt when *all* parents are visible.

Also note auto-compaction is disabled in `gc_marks_start` (`&& !rlgc_has_local`, `:6635` region) — compaction moves objects, incompatible with cross-objspace refs / `shared_bits` / the no-move shareable invariant. (Section 5 covers compaction.)

`gc_marks_finish` — `gc/default/default.c:6224`. Two RLGC accommodations:

```c
        /* RLGC: during a global GC the driver objspace's marked_slots accumulates objects
           marked in EVERY objspace ... so it is an aggregate that can exceed this one
           objspace's available slots. */
        GC_ASSERT(rlgc_global_gc_active || objspace_available_slots(objspace) >= objspace->marked_slots);
```

```c
    if (!objspace->local || objspace->flags.global_gc) {
        rb_ractor_finish_marking();
    }
```

**What/why:** (1) The `marked_slots >= available` invariant is per-objspace; under a global GC the driver's `marked_slots` is a VM-wide aggregate (every objspace's marks funnel through the driver's counter via `gc_aging`), so the assert is relaxed when `rlgc_global_gc_active`. (2) `rb_ractor_finish_marking` frees/clears a VM-global array under the VM lock; a lock-free local GC must not run it (two concurrent local GCs would double-free it), so it runs only in an STW collection (main objspace's GC, or a global GC) and a local GC defers it.

**How it fits:** This is the global-vs-local split surfacing in the mark-finish bookkeeping: the same code drives both a confined per-Ractor minor and the unified STW global mark, with `rlgc_global_gc_active` / `objspace->local` selecting the right invariant each time.

**Reviewer gotchas:**
- Relaxing a `GC_ASSERT` always deserves scrutiny: confirm the aggregate-counter reasoning is the *only* reason it can exceed, and that the per-objspace path (local GC) still enforces it. If a real over-count bug existed in a local GC it would still be caught.
- `gc_full_mark_clear_thunk` ignores its `data` arg and clears unconditionally — fine because it's only invoked under `rlgc_global_gc_active` (STW), so no concurrent writer.
- The `shared_bits` clear is suppressed under `RACTOR_LOCAL_GC_AUDIT` (the `&& !RACTOR_LOCAL_GC_AUDIT`): audit mode wants the bits to persist so `gc_shared_relation` can compare WB-recorded vs. mark-discovered (2.2).

### 2.7 `gc_mark_check_t_none`: the unchanged tripwire these guards exist to satisfy

`gc/default/default.c:5136` (called from `gc_mark` at `:5179`) — **this function is unchanged by the diff** (identical in base `de5545202`):

```c
static inline void
gc_mark_check_t_none(rb_objspace_t *objspace, VALUE obj)
{
    if (RB_UNLIKELY(BUILTIN_TYPE(obj) == T_NONE)) {
        ...
        rb_bug("try to mark T_NONE object (obj: %s, parent: %s)", obj_info_buf, parent_obj_info_buf);
    }
}
```

**Why it belongs in this section:** It is the assertion that fires when *any* of the above guards has a hole. `BUILTIN_TYPE(obj) == T_NONE` means marking reached a slot that was already **freed** (swept and returned to `T_NONE`) — i.e. a still-referenced object was incorrectly collected. Under RLGC every confinement/liveness bug (Family I confinement-miss, Family II cross-objspace subtree, Family III generational-WB) ultimately manifests here as `try to mark T_NONE object` — and the appended `parent: %s` is the diagnostic that points at the edge whose target was wrongly freed. The fact that it is *unchanged* is the point: the RLGC guards (foreign-skip, shared-roots, the sweep pin) exist precisely to keep this invariant true without weakening the check.

**Reviewer gotchas:**
- The ordering in `gc_mark` matters: the foreign-skip (2.1) returns *before* `gc_mark_check_t_none`. That's correct — a foreign object is never read, so we never assert on it — but it also means a confinement *miss* (an object that should have been foreign-skipped but wasn't, or one that should have been kept alive but was swept) is what trips this. When triaging a T_NONE bug under RLGC, the question is always "which objspace owns `obj`, and why wasn't it kept alive by that objspace's roots/shared_bits."
- `parent_object` must be accurate for the message to be useful; the root passes bracket it with `gc_mark_set_parent_invalid`/`_raw`.

---

## 3. shared_bits remset, write barrier & remembered set

This slice covers the data structure RLGC adds to track the **shareable -> unshareable boundary** (the `shared_bits` remset), how the write barrier populates it, how a local GC roots from it, and the ThreadSanitizer-driven hardening (atomic bitmap ops + byte-wide page flags) that makes concurrent local GCs safe. All of it lives in `gc/default/default.c`.

### 3.1 The page layout: `shared_bits`, `objspace`, byte-wide flags

`gc/default/default.c:871-913` (diff `@@ -829,11 +871`)

```c
    struct {
        unsigned int before_sweep : 1;
        /* ... single atomic byte store that cannot lose a concurrent set ... */
        unsigned char has_remembered_objects;
        unsigned char has_uncollectible_wb_unprotected_objects;
#if RACTOR_LOCAL_GC
        unsigned char has_shared_objects;
#endif
    } flags;
    rb_heap_t *heap;
#if RACTOR_LOCAL_GC
    rb_objspace_t *objspace;   /* owning objspace */
#endif
    ...
    bits_t remembered_bits[HEAP_PAGE_BITMAP_LIMIT];
#if RACTOR_LOCAL_GC
    bits_t shared_bits[HEAP_PAGE_BITMAP_LIMIT];   /* boundary remset */
#endif
```

**What:** Each page gains a `shared_bits` bitmap (one bit per slot, like `remembered_bits`), a back-pointer to its owning `objspace`, and the page-flag bitfield `has_remembered_objects` / `has_uncollectible_wb_unprotected_objects` is **widened from `:1` bitfields to full `unsigned char` bytes**, with a new `has_shared_objects` byte added.

**Why:** `shared_bits` is the remset for the new boundary RLGC must track: an *unshareable* object directly referenced by a *shareable* object. The `objspace` back-pointer is what lets a confined local GC tell its own objects from foreign ones (`GET_HEAP_OBJSPACE`, line 1010 of the macro block).

The bitfield->byte change is a **TSan fix, not cosmetic**. The old `:1` flags shared one storage word; under RLGC the lock-free write barrier and concurrent local GCs set these flags from *different Ractor threads at once*. Writing `flags.has_x = TRUE` on a bitfield is a non-atomic read-modify-write of the whole word, so a concurrent set of a *sibling* flag can be lost. Promoting each to its own byte makes `flags.has_x = TRUE` a single independent byte store. The comment at `:881` spells this out and cross-references the `shared_bits`/`remembered_bits` atomic fix below.

**Reviewer gotchas:**
- `has_shared_objects` and `shared_bits` are the "is this object reachable only via a shareable in another objspace?" signal. They are written by the WB (3.4), the audit-mode mark recompute (3.3), `rb_gc_impl_pin_shared` (3.6), and during compaction-move bookkeeping (line 1623 of the diff); read by `gc_mark_shared_roots` (3.5).
- Note the byte flags are still plain bytes, not `_Atomic` — correctness relies on each flag being an *independent* address so a torn neighbor is impossible; concurrent sets of the *same* flag are idempotent (always `TRUE`).

### 3.2 Atomic bitmap helper `gc_bitmap_atomic_set` / `MARK_IN_BITMAP_ATOMIC`

`gc/default/default.c:1003-1015` (diff lines 136-149)

```c
static inline bool
gc_bitmap_atomic_set(bits_t *bits, const struct heap_page *page, VALUE obj)
{
    volatile size_t *const word = (volatile size_t *)&bits[SLOT_BITMAP_INDEX(page, obj)];
    const size_t mask = (size_t)SLOT_BITMAP_BIT(page, obj);
    size_t old = 0;
    while ((old & mask) != mask) {
        const size_t prev = RUBY_ATOMIC_SIZE_CAS(*word, old, old | mask);
        if (prev == old) return true;
        old = prev;
    }
    return false;
}
#define MARK_IN_BITMAP_ATOMIC(bits, p)  gc_bitmap_atomic_set((bits), GET_HEAP_PAGE(p), (p))
```

**What:** An atomic `bitmap_word |= bit` returning `true` iff *this* call flipped the bit from clear.

**Why:** One `bits_t` word covers `BITS_BITLENGTH` slots. A plain `MARK_IN_BITMAP` is a non-atomic RMW, so two concurrent sets to the *same word for different objects* lose one update — and the lost bit belongs to *another object that happens to share the word*. For `remembered_bits` that means a still-referenced young object misses the next minor GC and is freed. The comment cites this as ThreadSanitizer-confirmed (the load-dependent `fibers_escaping` "mark T_NONE"). This is the same root cause recorded in MEMORY: under the lock-free RLGC model, *every multi-thread-written bitmap word needs atomic ops*.

**How it fits:** `bits_t` is pointer-width, so a `size_t` CAS covers the whole word. The clever detail (comment at `:1009`): the loop starts from `old = 0` so the **only** access to the word is the CAS itself — there is no separate non-atomic load that could race a concurrent setter. A spurious first miss just reloads the value the CAS atomically observed.

**Reviewer gotchas:**
- The `volatile size_t *` cast aliases the `bits_t` array; relies on `sizeof(bits_t) == sizeof(size_t)` (pointer-width). Worth confirming on any platform where that doesn't hold.
- Returns `false` both when *another* thread set it first and when it was already set — callers (3.7) treat "newly set by me" vs "not" only for the `remembered_bits` count bookkeeping.

### 3.3 Recomputing the boundary during the global mark: `gc_shared_relation`

`gc/default/default.c:861-876` (diff lines 861-876)

```c
static inline void
gc_shared_relation(rb_objspace_t *objspace, VALUE obj)
{
    VALUE parent = objspace->rgengc.parent_object;
    if (!SPECIAL_CONST_P(parent) &&
        RB_OBJ_SHAREABLE_P(parent) && !RB_OBJ_SHAREABLE_P(obj)) {
#if RACTOR_LOCAL_GC_AUDIT
        if (!MARKED_IN_BITMAP(GET_HEAP_SHARED_BITS(obj), obj)) gc_shared_wb_miss(...);
#endif
        MARK_IN_BITMAP_ATOMIC(GET_HEAP_SHARED_BITS(obj), obj);
        GET_HEAP_PAGE(obj)->flags.has_shared_objects = TRUE;
    }
}
```

**What:** Called per traversed edge during marking; whenever a *shareable* parent points at an *unshareable* child, (re)record the child in `shared_bits`.

**Why / how it fits:** `shared_bits` is cleared en masse and rebuilt from scratch by the unified mark of a global GC (see 3.8 — clears, and this re-derives). The WB maintains it incrementally between global GCs. In `RACTOR_LOCAL_GC_AUDIT` builds, this also *cross-checks* the WB: any boundary edge the WB failed to record is flagged as a write-barrier miss. That audit path is how Family I/III confinement-miss bugs in the memory log were surfaced.

### 3.4 Write barrier: populating `shared_bits`

`gc/default/default.c:7044-7066` (diff `@@ -6248,6 +7041`)

```c
#if RACTOR_LOCAL_GC
    if (RB_OBJ_SHAREABLE_P(b)) {
        MARK_IN_BITMAP_ATOMIC(GET_HEAP_SHARED_BITS(b), b);
        GET_HEAP_PAGE(b)->flags.has_shared_objects = TRUE;
    }
    else if (RB_OBJ_SHAREABLE_P(a) || MARKED_IN_BITMAP(GET_HEAP_SHARED_BITS(a), a)) {
        MARK_IN_BITMAP_ATOMIC(GET_HEAP_SHARED_BITS(b), b);
        GET_HEAP_PAGE(b)->flags.has_shared_objects = TRUE;
        rlgc_wb_shared_sets++;
        if (GET_HEAP_OBJSPACE(b) != rlgc_main_objspace) rlgc_wb_local_sets++;
    }
#endif
```

This runs in `rb_gc_impl_writebarrier(a, b)` (a gains a reference to b) *before* the existing generational retry logic. Two branches:

- **`b` is shareable:** mark `b` itself shared. A shareable may be referenced from another Ractor (e.g. a callcache reached through an inline cache in a shareable iseq), so `b`'s owner's local GC must keep it (and its subtree) alive. Shareables are pinned until the global GC, so recording `b` in `shared_bits` makes it a local-GC root.
- **`b` unshareable, but `a` is shareable (or `a` is itself a shared boundary object):** mark `b` shared. This is the core boundary case: `b` is reachable from a shareable referrer (directly, or transitively through another shared object such as a class's per-Ractor classext) that lives in an objspace the local GC never traverses. The `MARKED_IN_BITMAP(... a)` test propagates "sharedness" transitively down a chain of unshareable objects hanging off a shareable.

**Why this is sound (key invariant, comment at `:7045`):** The store can only be performed by `b`'s owner — Ractor isolation forbids holding a cross-Ractor *unshareable* reference — so the WB always sets the bit on *our own* object. The shareable `a` may live in another Ractor's space and is **never touched**. That is what keeps the lock-free WB confined.

**Reviewer gotchas:**
- The `MARKED_IN_BITMAP(GET_HEAP_SHARED_BITS(a), a)` read in the `else if` is a *non-atomic read* of another thread's potential writes, but `a` is the owner's own object (per the isolation argument), so the read is not actually cross-Ractor on the unshareable path. Worth confirming `a` can never be foreign here.
- This block is *unconditional* on GC mode — it runs even when no local GC exists. The cost is two reads + (rarely) an atomic CAS on every barrier-eligible store; the `rlgc_wb_*_sets` counters are debug-only.

### 3.5 Rooting from the remset in a local GC: `gc_mark_shared_roots`

`gc/default/default.c:6808-6843` (diff lines 1281-1331)

```c
static void
gc_mark_shared_roots(rb_objspace_t *objspace)
{
    ... ccan_list_for_each(&heap->pages, page, page_node) {
        if (!page->flags.has_shared_objects) continue;
        ... for each set bit in page->shared_bits:
            VALUE sobj = (VALUE)pp;
            gc_mark(objspace, sobj);
            if (RVALUE_OLD_P(objspace, sobj)) {
                gc_mark_children(objspace, sobj);
            }
```

Invoked from the root-marking path at `gc/default/default.c:5365-5372` (diff 1002-1006), guarded by `(objspace->local || rlgc_has_local) && !rlgc_global_gc_active`:

```c
    if ((objspace->local || rlgc_has_local) && !rlgc_global_gc_active) {
        MARK_CHECKPOINT("shared_roots");
        gc_mark_set_parent_raw(objspace, Qundef, false);
        gc_mark_shared_roots(objspace);
    }
```

**What:** A local GC root pass. It walks this objspace's pages with `has_shared_objects` set and, for every slot flagged in `shared_bits`, marks it — keeping alive the boundary objects reachable only through a shareable parent in *another* objspace (class method/constant caches, etc.) that this Ractor's normal roots don't reach.

**The OLD-boundary re-traversal (comment at `:6826`) is the subtle correctness fix.** A shared-root object that has aged to OLD is uncollectible, so `gc_mark` short-circuits (`gc_mark_set` returns "already marked") **without greying it** — its young children would then never be traversed by a minor GC. Unlike a normal old object, a shared-root object is rooted via `shared_bits`, **not** the remembered set, so `rgengc_rememberset_mark` doesn't cover it either. Without the explicit `gc_mark_children`, its young subtree gets swept while the cross-objspace shareable still references it — exactly the "dangling cross-objspace child / mark-T_NONE / freed class m_tbl,cc" crash family. So `gc_mark_shared_roots` re-traverses OLD shared roots' children directly, mirroring what `rgengc_rememberset_mark` does for remembered old objects.

**Why the global GC deliberately skips this** (comment at `:5374`): under a global STW GC, the unified mark walks full VM roots + cross-objspace edges and determines shareable liveness by *reachability*, so dead shareables can finally be reclaimed; `gc_shared_relation` rebuilds `shared_bits` from scratch as it marks (the bits were cleared at marks-start). Rooting from `shared_bits` there would wrongly pin dead shareables forever.

**Reviewer gotchas:**
- The guard `objspace->local || rlgc_has_local` means even the *main* objspace's GC roots from `shared_bits` once any local objspace exists — necessary because a main-objspace shareable can be referenced only from a worker.
- The `getenv("RLGC_DEBUG")` line per call is a debug `fprintf`; fine but it's a `getenv` on every shared-roots pass.

### 3.6 `rb_gc_impl_pin_shared` — explicit boundary pin

`gc/default/default.c:6845-6859` (diff lines 1338-1343)

```c
void
rb_gc_impl_pin_shared(VALUE obj)
{
    MARK_IN_BITMAP_ATOMIC(GET_HEAP_SHARED_BITS(obj), obj);
    GET_HEAP_PAGE(obj)->flags.has_shared_objects = TRUE;
}
```

Lets VM-internal infrastructure created under the VM lock (method/inline caches, class extensions) — reachable only through shareables the local GC never traverses — be forced into the boundary remset so the owner's local GC keeps it alive. This is the explicit-pin counterpart to the WB's automatic recording; it backs several of the confinement-miss fixes (Faces B/D and friends in the task list).

### 3.7 Remembered-set producer made atomic: `rgengc_remembersetbits_set`

`gc/default/default.c:6738-6748` (diff `@@ -6031,14 +6738`)

```c
    const bool newly = gc_bitmap_atomic_set(bits, page, obj);
    page->flags.has_remembered_objects = TRUE;
    return newly ? TRUE : FALSE;
```

**What:** Replaces the old `MARKED_IN_BITMAP` test + non-atomic `MARK_IN_BITMAP` with the atomic helper.

**Why / ordering:** The old non-atomic test-then-`|=` dropped bits of other objects sharing the word (3.2). Critically, the new code **sets the bit FIRST, then the page flag**. This is the producer half of a race protocol with `rgengc_rememberset_mark` (3.8): the consumer *clears the flag before draining the bits*, so by setting bit-then-flag here, a concurrent drain either sees our flag (and rescans) or our bit lands on a word it's about to/has zeroed atomically — never silently skipped.

### 3.8 Remembered-set consumer drain: `rgengc_rememberset_mark`

`gc/default/default.c:6894-6912` (diff `@@ -6122,11 +6894`)

```c
            page->flags.has_remembered_objects = FALSE;   /* clear flag BEFORE draining */
            for (j=0; j < (size_t)bitmap_plane_count; j++) {
#if RACTOR_LOCAL_GC
                const bits_t rem = (bits_t)RUBY_ATOMIC_SIZE_EXCHANGE(*(volatile size_t *)&remembered_bits[j], 0);
#else
                const bits_t rem = remembered_bits[j];
                remembered_bits[j] = 0;
#endif
                bits[j] = rem | (uncollectible_bits[j] & wb_unprotected_bits[j]);
            }
```

**What:** Two changes. (1) `has_remembered_objects = FALSE` **moved to before** the bit drain (it was after, see the deleted line at diff 1369). (2) The per-word "read remembered bit, then zero it" becomes a single `RUBY_ATOMIC_SIZE_EXCHANGE` (atomic read-and-clear) under RLGC.

**Why (comment at `:6896`):** A concurrent lock-free WB on another Ractor remembers an object on this page by *bit-then-flag* (3.7). Clearing the flag first here means: if such a set races in *after* our flag-clear, the flag goes back to `TRUE` and the page is rescanned next time — nothing is lost. The atomic exchange ensures a racing `MARK_IN_BITMAP_ATOMIC` either lands before the exchange (drained now) or after it (lands on the zeroed word, kept for next pass) — never a lost-update tear. This pairs exactly with 3.7's ordering to form a complete lock-free producer/consumer handshake.

**Reviewer gotchas:**
- The flag-clear-before-drain ordering is load-bearing and easy to "tidy" back to the old position — it must stay before the loop.
- The non-RLGC path is unchanged (plain `= 0`), so the atomic cost is RLGC-only.

### 3.9 Clearing/recomputing `shared_bits` only when safe

`gc/default/default.c:6940-6956` (diff `@@ -6158,6 +6940`)

```c
#if RACTOR_LOCAL_GC && !RACTOR_LOCAL_GC_AUDIT
    if (!rlgc_has_local || rlgc_global_gc_active) {
        memset(&page->shared_bits[0], 0, HEAP_PAGE_BITMAP_SIZE);
        page->flags.has_shared_objects = FALSE;
    }
#endif
```

In `rgengc_mark_and_rememberset_clear`. **What:** `shared_bits` is wiped (to be rebuilt by `gc_shared_relation` during the unified mark) **only** in the pure single-objspace case or under a *global* GC. **Why:** A per-Ractor *local* GC cannot see foreign shareable parents, so it must not clear the boundary remset — shareables stay pinned via `shared_bits` until the next global GC reclaims them. This is the clear-side counterpart of the root-side guard in 3.5: local GCs *read but never clear* `shared_bits`; only the all-objspace-visible global mark recomputes it. Note it is also skipped in AUDIT builds (so the audit can compare WB-maintained bits against the mark).

### 3.10 Acknowledged design choice: cross-objspace old->young edges are NOT remembered

`gc/default/default.c:5743-5750` (diff `@@ -5124,6 +5740`)

```c
#if RACTOR_LOCAL_GC
    /* RLGC: a cross-objspace old->young edge is NOT covered by the generational remembered set. ... */
    if (!SPECIAL_CONST_P(child) && GET_HEAP_OBJSPACE(child) != GET_HEAP_OBJSPACE(parent)) return;
#endif
```

This is in `check_generation_i`, the `RGENGC_CHECK_MODE` verifier that asserts every old->young edge is in the remembered set. **What:** Under RLGC, a cross-objspace old->young edge is *intentionally* excluded — the child lives in and is kept alive by another objspace; the unshareable->shareable boundary is handled by `shared_bits`, and the local GC foreign-skips foreign objects. So such an edge is **not** a write-barrier miss and the verifier must not flag it.

**Reviewer gotchas:** This is the explicit boundary between the two mechanisms — *within*-objspace edges -> generational remembered set (3.7/3.8); *cross*-objspace boundary -> `shared_bits` (3.4/3.5). A reviewer should sanity-check that nothing relies on the remembered set for cross-objspace liveness (it's `shared_bits`' job), and that this early-`return` doesn't mask a *genuine* within-objspace miss (it only returns when the two objspaces differ). Per the MEMORY note, the `RGENGC_CHECK_MODE` verifier cannot catch the concurrency races anyway — TSan is the tool for those (3.2).

---

**Cross-cutting reviewer notes for this slice:**
- The whole design hinges on one isolation invariant: an unshareable object is only ever written by its owner Ractor, so the WB always sets `shared_bits` on a *local* object and never touches the foreign shareable referrer. If that invariant is ever violated, the lock-free WB becomes a cross-Ractor data race.
- Three bitmap words are now multi-writer: `shared_bits`, `remembered_bits` (both via atomic CAS/exchange) and the page byte-flags (independent bytes). Any *new* page bitmap that the WB or a concurrent local GC writes must follow the same atomic discipline — this is the generalized lesson from the TSan fix.
- `gc_mark_shared_roots`' OLD re-traversal (3.5) and the `gc_shared_relation`/clear guards (3.3, 3.9) together implement "local GC pins shareables; global GC reclaims them." Breaking the `rlgc_global_gc_active` guard on either side either leaks dead shareables (clear in a local GC) or frees live boundary subtrees (root in a global GC).

---

## 4. Compaction disabled, finalizer ownership routing, misc (`gc/default/default.c`)

Three independent concerns live in this file's slice of the RLGC diff: (1) **compaction is forced off** whenever a per-Ractor objspace exists, because moving objects is fundamentally incompatible with cross-objspace references; (2) **finalizers are routed to the owner objspace's table**, since `obj` may be owned by a Ractor other than the caller; and (3) a scattering of **per-objspace plumbing** (newobj-cache flush, finalizer-table marking, consistency-check gating) follows from the global GC needing to sweep every objspace at once. Everything below is guarded by `#if RACTOR_LOCAL_GC` and is a no-op in stock CRuby.

A small piece of infrastructure underlies the finalizer routing — note it first, because it explains why the routing code looks the way it does:

### Why a `rlgc_finalizer_table()` accessor exists at all
`gc/default/default.c:165`
```c
/* Accessor for ANOTHER objspace's finalizer_table — must be defined BEFORE the macro below, which
 * rewrites the bare token `finalizer_table` to `objspace->finalizer_table`. */
static inline st_table *rlgc_finalizer_table(rb_objspace_t *os) { return os->finalizer_table; }
#define finalizer_table 	objspace->finalizer_table
```
This file pervasively uses `#define finalizer_table objspace->finalizer_table` so unqualified code reads "the current objspace's table." The instant you need *another* objspace's table, that macro is actively wrong — `finalizer_table` can only ever name the local `objspace`. The inline accessor is the escape hatch, and it is deliberately defined one line **above** the macro (otherwise the macro would rewrite the `os->finalizer_table` inside the accessor body). The same pattern is used for `rlgc_get_during_gc`/`rlgc_set_during_gc` at line 158. **Reviewer gotcha:** any new cross-objspace field access must use an accessor declared before its `#define`; relying on the bare macro silently targets the wrong objspace.

---

### Compaction off — `GC.compact` (Face-E sibling)
`gc/default/default.c` (diff line 1640, function `gc_compact`)
```c
bool compact = true;
#if RACTOR_LOCAL_GC
    if (rlgc_has_local) compact = false;
#endif
    rb_gc_impl_start(objspace, true, true, true, compact);
```
**What:** `GC.compact` normally runs a full GC with `compact=true`. Under RLGC it downgrades to a non-moving full GC and still returns `gc_compact_stats(self)` (which will honestly report 0 objects moved). **Why:** compaction relocates objects; under RLGC that would invalidate cross-objspace pointers, the `shared_bits` per-page remset (keyed by slot address), and the invariant that shareables never move from their home objspace. **How it fits:** rather than erroring, `GC.compact` degrades gracefully to "collect, don't move." **Gotcha:** the gate is `rlgc_has_local` (set the first time a non-main objspace is created), not "am I a local Ractor" — once *any* Ractor exists, even a main-Ractor `GC.compact` must not move.

### Compaction off — `GC.verify_compaction_references`
`gc/default/default.c` (diff line 1661)
```c
if (rlgc_has_local) {
    rb_gc_impl_start(objspace, true, true, true, false);
    return gc_compact_stats(self);
}
```
Same rationale, early-returned at the top of the function so the move-and-verify machinery (which would crash on cross-objspace references) is never entered.

### Auto-compaction off — the two `ruby_enable_autocompact` use-sites (the actual Face-E fix)
`gc/default/default.c` (diff lines 1210 and 1475)
```c
if (ruby_enable_autocompact
#if RACTOR_LOCAL_GC
    && !rlgc_has_local
#endif
    ) {
    objspace->flags.during_compacting |= TRUE;
}
```
This is the same `&& !rlgc_has_local` guard added at **both** places `during_compacting` is armed from the auto-compact flag: `gc_marks_start` (major-GC path) and `gc_start` (explicit-enable path). **Why gate at the point of use rather than at the setter:** `GC.auto_compact=true` can be set at boot, *before* any Ractor — and thus before `rlgc_has_local` becomes true — so gating in the setter would be too early. The comment spells this out. This is precisely Face E from the design summary (the unguarded `auto_compact=true` that previously let a compaction run under RLGC and corrupt the heap). **Reviewer gotcha:** these are the *complete* set of arming sites; if a future patch adds a third place that sets `during_compacting` from autocompact, it must carry the same guard, or compaction silently re-enables under RLGC. Note `gc_is_moveable_obj` and the rest of the compaction machinery were left **unchanged** — they're simply unreachable now, which keeps the diff small and the moving code path intact for non-RLGC builds.

---

### Finalizer routing — `rb_gc_impl_define_finalizer` (Face F)
`gc/default/default.c:3307` (diff line 587)
```c
#if RACTOR_LOCAL_GC
    /* The finalizer entry must live in obj's OWNER objspace's table: run_final() looks it up there
     * during that objspace's sweep, and obj may be owned by a different Ractor than the caller (e.g.
     * a finalizer defined on a shareable object). Mirrors rb_gc_impl_copy_finalizer(). */
    st_table *const ftbl = rlgc_finalizer_table(GET_HEAP_OBJSPACE(obj));
#else
    rb_objspace_t *objspace = objspace_ptr;
    st_table *const ftbl = finalizer_table;
#endif
```
**What:** the entry `[obj_id, proc]` is inserted into the table of the objspace that *owns `obj`* (`GET_HEAP_OBJSPACE(obj)` walks `obj`'s page to its owning objspace), not the caller's `objspace_ptr`. The rest of the function then uses the local `ftbl`. **Why:** finalizers fire during the owner's sweep (`run_final` at line 3416 looks up the entry via the bare `finalizer_table` macro, i.e. *its own* objspace's table). If you define a finalizer on a **shareable** object — which lives in the main/home objspace — from a worker Ractor, the caller's `objspace_ptr` is the worker's, but the object is swept by the home objspace. Routing to `GET_HEAP_OBJSPACE(obj)` keeps define-side and sweep-side looking at the same table. This is Face F.

### Finalizer routing — `rb_gc_impl_undefine_finalizer`
`gc/default/default.c:3374` (diff line 621)
```c
st_table *const ftbl = rlgc_finalizer_table(GET_HEAP_OBJSPACE(obj));
...
st_delete(ftbl, &data, 0);
```
The symmetric delete from the owner's table. Note the non-RLGC `objspace` local was *removed* from the function head and reintroduced only inside the `#else` — so an accidental use of the bare `finalizer_table` macro under RLGC would fail to compile rather than silently target the wrong table. That's a deliberate compile-time guard.

### Finalizer routing — `rb_gc_impl_copy_finalizer` (the two-objspace case)
`gc/default/default.c:3374` (diff line 647)
```c
rb_objspace_t *const src_objspace  = GET_HEAP_OBJSPACE(obj);
rb_objspace_t *const dest_objspace = GET_HEAP_OBJSPACE(dest);
...
if (RB_LIKELY(st_lookup(rlgc_finalizer_table(src_objspace), obj, &data))) {
    table = rb_ary_dup((VALUE)data);
    RARRAY_ASET(table, 0, rb_obj_id(dest));
    st_insert(rlgc_finalizer_table(dest_objspace), dest, table);
```
**What:** copy is the only finalizer op where two distinct objspaces can be in play at once — read from `obj`'s table, write to `dest`'s table. **Why:** copying happens e.g. when a Ractor clones a shareable object that lives in the main objspace: source entry is in main's table, but the clone is owned by the Ractor and must finalize under the Ractor's sweep, so the new entry goes into `dest`'s table. The single `finalizer_table` macro can't express "two different tables," which is exactly why the accessor exists. **Reviewer gotcha:** `dest` must already be a heap object with a resolvable page when this runs (it is — copy happens post-allocation); `GET_HEAP_OBJSPACE` on a not-yet-paged value would be undefined.

### Per-objspace finalizer-table marking under a global GC
`gc/default/default.c` (diff lines 958 and 987)
```c
static void
gc_mark_other_objspace_finalizer_table_i(void *os_ptr, void *driver_ptr)
{
    rb_objspace_t *const os = os_ptr;
    if (os == driver_ptr) return; // the driver's own table is marked by mark_roots
    st_table *const ft = rlgc_finalizer_table(os);
    if (ft != NULL) st_foreach(ft, pin_value, (st_data_t)driver_ptr);
}
...
// in mark_roots():
if (rlgc_global_gc_active) {
    rb_gc_foreach_objspace(gc_mark_other_objspace_finalizer_table_i, objspace);
}
```
**What:** `finalizer_table` is per-objspace; `mark_roots` only pins the *driver* objspace's table (the existing `st_foreach(finalizer_table, pin_value, ...)` just above). A global GC clears-and-sweeps **every** objspace, so without this every *worker's* finalizer array — which is live only via that worker's table, an internal hidden array not reachable from normal roots — would be swept, dangling the table and producing a UAF in `run_final` or a "mark T_NONE" at that worker's next local GC. **How it fits:** the routing fix (Face F) put entries in the right tables; this ensures the global GC actually *keeps those tables' values alive*. The two halves are a pair. Marking cross-objspace is sound here precisely because `rlgc_global_gc_active` is set (STW, the foreign-skip is lifted, `pin_value` pins onto the value's own page). **Gotcha:** this only runs when `rlgc_global_gc_active` — a *local* GC marks only its own table (correct: it sweeps only its own objspace).

---

### Consistency-check gating — `gc_verify_internal_consistency_maybe`
`gc/default/default.c` (diff lines 676, 1108, 1151, 1162, 1467, 1522)
The diff replaces every `#if RGENGC_CHECK_MODE >= 2 / gc_verify_internal_consistency(objspace) / #endif` block with a single `gc_verify_internal_consistency_maybe(objspace)` call. **Why:** the verifier is RLGC-aware (it must skip cross-objspace edges, which the write barrier intentionally does not track — see `check_generation_i`/`check_color_i` at diff lines 1020/1035, which early-`return` when `GET_HEAP_OBJSPACE(child) != GET_HEAP_OBJSPACE(parent)`) and can be toggled at runtime (`RUBY_GC_VERIFY=1`, per the MEMORY note). Folding the compile-time `#if` into one helper centralizes that gating. **Gotcha for reviewers:** as the MEMORY entry records, this snapshot verifier *cannot* catch concurrency races (the GC barrier masks the window); it's a structural check only. Don't read its presence as race coverage.

### Newobj-cache flush takes an explicit objspace
`gc/default/default.c` (diff line 750, `gc_ractor_newobj_cache_clear`)
```c
-    rb_objspace_t *objspace = rb_gc_get_objspace();
+    rb_objspace_t *objspace = (rb_objspace_t *)data;
```
**What:** the cache-flush callback now flushes into the objspace passed as `data` instead of `rb_gc_get_objspace()`. **Why:** a Ractor's newobj cache holds an in-progress page + freelist whose slots physically live in *that Ractor's* objspace; appending them to a foreign heap corrupts both. Callers now pass the owning objspace: `gc_sweep_start` (diff 762) and `after_fork` (diff 1716, via `gc_after_fork_flush_objspace`) flush each cache only into its own objspace during a global sweep. **Gotcha:** this is the linchpin that makes "global GC sweeps all objspaces in turn" correct — each cache is flushed exactly once, when *its* objspace is swept, never into the driver's.

### Note on "each_objects across objspaces"
`rb_gc_impl_each_objects` (`default.c:3278`) itself is **unchanged** — it still iterates a single objspace via `objspace_each_objects`. The cross-objspace iteration in this diff is done one level up by `rb_gc_foreach_objspace(thunk, …)` (e.g. `gc_full_mark_clear_thunk` at diff 1196 clearing mark/old/remembered/shared bits in every objspace at the start of a global mark, and the finalizer-table thunk above). So "global GC touches all objspaces" is composed from per-objspace primitives plus a foreach driver, not from a modified `each_objects`. A reviewer auditing "does the global GC really cover every objspace?" should follow the `rb_gc_foreach_objspace` call sites, all gated on `rlgc_global_gc_active`.

---

## 5. `gc.c` — interface layer: roots, orphan list, keep-alives, id2ref

`gc.c` is the GC-impl-agnostic interface between the VM and the pluggable GC backend (`gc/default/default.c`, MMTk). Under RLGC its job is to (1) route allocations/marking to the *current Ractor's* objspace, (2) expose iteration over *all* objspaces for the global GC, and (3) patch the historical "VM globals live in the main objspace" assumption everywhere a confined local GC would otherwise read or free shared state. The two private flags it gates on are defined in the default impl: `rlgc_has_local` (any local objspace exists) and `rlgc_global_gc_active` (a STW global GC is running) — `gc/default/default.c:1558-1559`.

### 5.1 Objspace routing: `rb_gc_get_objspace`

`gc.c:246-252`
```c
rb_ractor_t *cr = rb_current_ractor_raw(false);
if (cr != NULL && cr->local_gc_objspace != NULL) {
    return cr->local_gc_objspace;
}
return GET_VM()->gc.objspace;
```
**What/why:** the single most load-bearing change. Every `rb_gc_get_objspace()` caller in `gc.c` now resolves to *the calling Ractor's own heap*, not the VM-wide one. It falls back to `vm->gc.objspace` during early boot (no main Ractor yet) and for threads with no current Ractor. **Gotcha:** because this is now Ractor-relative, any code path that calls it from a *different* Ractor than the one owning `obj` is a latent confinement bug — most of the rest of this section exists to repair those.

### 5.2 The orphan list: `rb_gc_orphan_local_objspace` + `rb_gc_foreach_objspace`

`gc.c:301-313` (orphan entry + handoff)
```c
struct rb_orphan_objspace_entry { void *objspace; struct ccan_list_node node; };
static CCAN_LIST_HEAD(rb_gc_orphaned_objspaces);
...
void rb_gc_orphan_local_objspace(void *objspace) {
    if (objspace == NULL || objspace == GET_VM()->gc.objspace) return;
    struct rb_orphan_objspace_entry *e = malloc(sizeof(struct rb_orphan_objspace_entry));
    if (e == NULL) return; /* out of memory: leave it as before (unwalked) */
    e->objspace = objspace;
    ccan_list_add_tail(&rb_gc_orphaned_objspaces, &e->node);
}
```
`gc.c:319-340` (`rb_gc_foreach_objspace`)
```c
func(main_objspace, data);
if (!ruby_single_main_ractor) {
    ccan_list_for_each(&vm->ractor.set, r, vmlr_node) {
        if (r->local_gc_objspace != NULL && r->local_gc_objspace != main_objspace)
            func(r->local_gc_objspace, data);
    }
}
ccan_list_for_each(&rb_gc_orphaned_objspaces, e, node) { func(e->objspace, data); }
```
**Why this is the centerpiece fix (Family I / cc_tbl UAF):** the in-file comment (`gc.c:283-298`) spells out the exact crash this prevents. When a Ractor terminates, `vm_remove_ractor` drops it from `vm->ractor.set` but its objspace is *not* freed — it can still own a shareable (e.g. a class sent to main). If the global GC couldn't reach that orphaned objspace, the class's mark bit never clears, the unified global mark short-circuits on the "already marked" class, and the global sweep frees its still-installed `cc_tbl` → UAF in the next method-cache lookup. The orphan list keeps such objspaces walkable. `rb_gc_foreach_objspace` is the canonical "every live heap" iterator: main + each live Ractor + every orphan.

**How it fits the design:** orphaned objspaces are *only* ever walked by the global GC (STW, all Ractors stopped), which is why the handoff comment says "iterated only under the global-GC barrier, so no extra locking is needed here beyond the VM lock the caller holds." The `malloc` (not Ruby alloc) is deliberate — it runs mid-teardown and must not trigger a GC.

**Reviewer gotchas:**
- Orphans are never removed and their non-dead shells persist forever ("Freeing a fully-empty orphan is a future optimization"). A long-lived program that spawns and kills many Ractors holding shareables accumulates objspace shells; the *objects* are reclaimed but the list grows monotonically. Worth flagging as a known leak.
- `rb_gc_foreach_objspace`, `rb_objspace_each_objects_all_ractors` (`gc.c:4170-4189`), `rb_gc_conservative_owner` (`gc.c:3389-3413`), and `rb_gc_ractor_newobj_cache_foreach_for_objspace` (`gc.c:351-365`) all hand-roll the *same* three-part walk (main / `ractor.set` / orphan list) with the *same* barrier precondition. They are correct but duplicated — a single misuse without the barrier in any one of them is a data race on the Ractor set. Confirm every caller is on a STW path.

### 5.3 `rb_gc_object_in_current_objspace_p` and `rb_gc_conservative_owner`

`gc.c:3422-3427`
```c
bool rb_gc_object_in_current_objspace_p(VALUE obj) {
    if (SPECIAL_CONST_P(obj)) return true;
    return rb_gc_impl_pointer_to_heap_p(rb_gc_get_objspace(), (const void *)obj);
}
```
**What/why:** a *barrier-free* "does this object live in MY heap?" test. It inspects only the current objspace's own page set, which is stable on the owning thread — so unlike `rb_gc_conservative_owner` (which walks all objspaces and *needs* the VM barrier, `gc.c:3389-3413`) it is safe to call from a running Ractor. It backs both the Ractor receive-path re-materialization decision and the keep-alive helper below. **Gotcha:** the semantics for non-RLGC builds are "always true" (single objspace) — verify callers treat `true` as "no cross-objspace action needed," which is what the receive path does (skips re-materialization correctly).

### 5.4 The local-GC branch of `rb_gc_mark_roots` (the heart of confinement)

`gc.c:3460-3511`. The early return is the entire confined-marking contract:
```c
if (objspace != vm->gc.objspace && !rlgc_global_gc_active) {
    rb_ractor_t *cr = rb_ec_ractor_ptr(ec);
    MARK_CHECKPOINT("local_ractor");
    rb_gc_mark_ractor_local_roots(cr);            // received msgs, local storage, std IO, this Ractor's threads
    MARK_CHECKPOINT("machine_context");
    mark_current_machine_context(ec);             // conservative stack of the GC-triggering thread
    ...keep-alives (Face D + trap)...
    return;                                        // <-- never reaches rb_vm_mark / global roots
}
```
**Why:** a local GC (a) is not the main objspace and (b) is not the global GC. It marks *only* the current Ractor's execution roots and its own machine context, then returns *before* `rb_vm_mark(vm)`, `end_proc`, `global_tbl`, `vm->mark_object_ary`, etc. This encodes the design's core assumption: **VM-global roots live in the main objspace and are kept alive by the main/global GC**, so a worker's confined GC must skip them (reading another objspace's roots, or freeing a shared table it happens to own, is the bug class this whole branch exists to avoid). `mark_current_machine_context(ec)` is restricted to the *current* thread's stack — the Ractor's other threads are reached structurally via `rb_gc_mark_ractor_local_roots` (defined in `ractor.c:274`, which also calls `rb_gc_mark_thread_roots` per thread).

**The keep-alive list — exceptions to "VM globals live in main" (Face D + trap):** `gc.c:3493-3506`
```c
gc_keepalive_vm_global_if_local(id2ref_value);
gc_keepalive_vm_global_if_local(rb_gc_vm_global_fstring_table());
gc_keepalive_vm_global_if_local(rb_gc_vm_global_symbol_set());
gc_keepalive_vm_global_if_local(rb_gc_vm_global_symbol_ids());
for (int i = 0; i < RUBY_NSIG; i++) {
    gc_keepalive_vm_global_if_local(vm->trap_list.cmd[i]);
}
```
with the helper (`gc.c:3416-3421`):
```c
static void gc_keepalive_vm_global_if_local(VALUE obj) {
    if (obj && rb_gc_object_in_current_objspace_p(obj)) rb_gc_mark(obj);
}
```
**Why these specific objects:** the "VM globals live in main" assumption has *exceptions* — tables that are *not* WB-protected and get *reallocated into whichever Ractor crosses a resize/load-factor threshold*:
- `id2ref_value` — the `_id2ref` st_table wrapper, allocated in whatever objspace first calls `_id2ref`.
- the fstring dedup table and the symbol set/ids — `concurrent_set`/array backings that a resize relocates into the Ractor that triggered the grow.
- `vm->trap_list.cmd[]` (the trap exception): `Signal.trap` is allowed off the main Ractor; a Proc handler is made shareable (pinned) but a **String** command handler is a plain this-objspace String reachable *only* through the VM-global slot.

Because the global-roots mark was skipped by the early `return`, any of these that physically lives in *this* worker's objspace would be swept while still installed and concurrently used by other Ractors → "SEGV in st_insert", "Object ID seen, but not in `_id2ref` table", or signal delivery eval'ing a freed String. The helper marks it *only if it's local* (`rb_gc_object_in_current_objspace_p`); a foreign copy is left to its true owner / the global GC.

**Reviewer gotchas:**
- The keep-alive list is a denylist of *known* relocatable VM globals — it is exactly the set of confinement-miss faces found so far. The open task list (Faces for `end_procs`, `vm->coverages`, `mark_object_ary`, thread-variable host) shows this list is **incomplete by construction**: any other non-WB-protected VM-global container whose elements can land in a worker objspace is a latent UAF that this branch will *not* catch. When reviewing, treat "is there a VM-global root the worker could own that isn't in this list?" as the standing question.
- `vm->global_hooks` is *deliberately* not marked here (`gc.c:3476-3483`): it is a shared list mutated lock-free by foreign Ractors (`hook_list_connect`), so a confined GC iterating it would race the writer. It is left to the STW global GC. Make sure no future change "helpfully" adds it.
- Reads of `vm->trap_list.cmd[i]` race a concurrent `Signal.trap`, but the comment relies on the read being a single aligned pointer (atomic) — correct on the supported platforms, but it is an *unsynchronized* read; flag if portability to a platform without atomic pointer loads is ever in scope.

### 5.5 `id2ref_tbl` locking + keep-alive (VM-global table swept by a worker)

The `_id2ref` table is registered in `global_object_list`, which a lock-free local GC's roots do *not* mark — so its wrapper can be swept by a worker GC while VM-lock-protected inserters/readers run. Every touch is now lock-guarded with the **non-barrier** lock and re-checks the global pointer:

`gc.c:2192-2204` (`id2ref_tbl_free`)
```c
RB_VM_LOCKING_NO_BARRIER() {
    id2ref_tbl = NULL; // clear under the lock so inserters re-checking inside the lock skip it
    st_free_table(table);
}
```
`gc.c:2278-2280` (`object_id0`) and `gc.c:2144-2146` (`rb_gc_obj_id_moved`) both add an *inside-the-lock re-check*:
```c
RB_VM_LOCKING() { if (id2ref_tbl) st_insert(id2ref_tbl, ...); }   // re-check under lock
```
`gc.c:2420-2428` (`obj_free_object_id`) moves the `st_delete` under the non-barrier lock and captures the result:
```c
int id2ref_deleted;
RB_VM_LOCKING_NO_BARRIER() { id2ref_deleted = st_delete(id2ref_tbl, (st_data_t *)&obj_id, NULL); }
if (!id2ref_deleted) { ... }
```
**Why `NO_BARRIER`:** `id2ref_tbl_free` and `obj_free_object_id` run *inside a GC sweep*, which is **not a safepoint**. A barrier-aware `RB_VM_LOCKING` would, if a global GC is pending, *join the barrier mid-sweep*, leaving the objspace half-collected for the global GC to walk. The non-barrier lock still gives mutual exclusion with inserters/readers and with other local GCs (all serialize on `vm->ractor.sync`) without joining the barrier. **The double-check pattern is essential and easy to get wrong:** the free clears `id2ref_tbl = NULL` under the lock, so every inserter must re-test `id2ref_tbl` *after* acquiring the lock (a TOCTOU between the outer `RB_UNLIKELY(id2ref_tbl)` fast check and the locked body). **Gotcha:** any *new* `id2ref_tbl` access must follow this exact "lock + re-check non-NULL" shape; a raw `st_insert` outside it reintroduces the race.

### 5.6 `gc_mark_generic_ivar_sync` (Family III adjacency — generic ivars)

`gc.c:3651-3670`
```c
static inline void gc_mark_generic_ivar_sync(VALUE obj) {
#if RACTOR_LOCAL_GC
    if (rlgc_has_local && !rlgc_global_gc_active) {
        RB_VM_LOCKING_NO_BARRIER() { rb_mark_generic_ivar(obj); }
        return;
    }
#endif
    rb_mark_generic_ivar(obj);
}
```
Now called from `rb_gc_mark_children` and `rb_gc_move_obj_during_marking` in place of the bare `rb_mark_generic_ivar` (`gc.c:3688`, `gc.c:3724`). **Why:** writers mutate the VM-global `generic_fields_tbl_` under the VM lock (which can rehash/realloc the shared st_table); a confined local GC marks *without* the STW lock, concurrently with those writers, so its lock-free `st_lookup` could read a bucket a writer is moving → torn read marking a freed slot. The non-barrier lock makes the lookup mutually exclusive with writers and other local GCs. **Why non-barrier (same reasoning as 5.5):** a barrier-aware lock would join a pending global-GC barrier *mid-mark*, leaving a stale `generic_fields_tbl` entry whose `imemo_fields` was already swept → "mark T_NONE". It is correctly skipped when no local objspace exists (single-Ractor / MMTk: `rlgc_has_local` false) and during the global GC (lock already held, no writer runs). **Gotcha:** this guards the *generic ivar table* read but is orthogonal to the open Family-III rememberset/WB faces (tasks #17/#18) — do not assume it covers cross-objspace generational write-barrier correctness; it only serializes the table read.

### 5.7 `RB_GC_MARK_OR_TRAVERSE` — gating the foreign `mark_func_data`

`gc.c:2975-2986`
```c
void *objspace = rb_gc_get_objspace();           // was: vm->gc.objspace
if (LIKELY(vm->gc.mark_func_data == NULL) || rb_gc_impl_during_gc_p(objspace)) {
    GC_ASSERT(rb_gc_impl_during_gc_p(objspace));
    (func)(objspace, (obj_or_ptr));
}
```
**Why:** `vm->gc.mark_func_data` redirects marking to a callback (`ObjectSpace.reachable_objects_from`, the Ractor shareability check) and is **VM-global**. With lock-free per-Ractor GCs, Ractor A may set it (while *not* in a GC) concurrently with Ractor B's real local GC — which would hijack B's marking into the foreign callback (which then *allocates* → "allocation during GC" / corruption). The fix gates on `during_gc_p(objspace)`: a real GC always has `during_gc` set on its own objspace, so it actually-marks regardless of a foreign `mark_func_data`. The hot path (`mark_func_data == NULL`) still short-circuits, so `during_gc_p` is only evaluated on the redirect path. Note the objspace source also changed to `rb_gc_get_objspace()` so the `during_gc` check is read against the *correct* (current Ractor's) objspace.

### 5.8 Newobj-cache and objspace lifecycle helpers (supporting cast)

These make allocation/teardown objspace-correct rather than "current-thread-objspace"-correct:
- `rb_gc_ractor_cache_alloc` / `rb_gc_ractor_cache_free` (`gc.c:3994-4006`) now allocate/free a Ractor's newobj cache against `ractor->local_gc_objspace` (its *own* heap), not whatever objspace is current on the teardown thread — otherwise freelist slots return to the wrong heap. **Gotcha:** the signature of `rb_gc_ractor_cache_free` changed from `(void *cache)` to `(rb_ractor_t *r)` so it can recover the right objspace; check all callers were updated.
- `rb_gc_ractor_newobj_current_cache_foreach` (`gc.c:280-288`) flushes *only* the current Ractor's cache during a local GC — iterating all caches would mishandle other Ractors' freelists (they point into other objspaces). Its sibling `..._for_objspace` (`gc.c:351-365`) is the global-GC version that flushes each cache *into the objspace it belongs to*.
- `rb_gc_objspace_alloc_local` / `rb_gc_objspace_free_local` / `rb_gc_ractor_cache_alloc_on_main` / `rb_gc_rlgc_enabled` (`gc.c:4007-4047`) are the lifecycle + the `RUBY_RACTOR_LOCAL_GC` env toggle. Note the design comment in 5.2 says `rb_gc_objspace_free_local` is "unused" (orphans are never freed) — it exists but the orphan path keeps shells alive, so confirm whether any path actually calls it (dead code vs. future use).

### 5.9 Out-of-slice cross-references (not in `gc.c`)

The task brief mentions `rb_gc_mark_thread_roots` and the root-fiber saved-context (`cont_mark`) marking. These are **not in `gc.c`** — `rb_gc_mark_thread_roots` is defined in `vm.c:3851` and invoked from `ractor.c:286` inside `rb_gc_mark_ractor_local_roots` (the local-roots entry point that *this* section's `rb_gc_mark_roots` branch calls at `gc.c:3470`). The companion section covering `vm.c`/`ractor.c`/`cont.c` should detail the root-fiber saved-context marking; from the `gc.c` side the only thing to verify is that the local-GC branch's single call to `rb_gc_mark_ractor_local_roots(cr)` is the *sole* structural entry into a Ractor's threads/fibers, and that it marks the current thread's machine stack separately via `mark_current_machine_context(ec)` because the thread wrapper objects (`th->self`) may live foreign (in main) and be skipped by the confined mark.

---

## 6. Message ownership: materialize-on-receive & in-flight pinning

A Ractor message that is *copied* or *moved* (not a shareable `ref`) is the hardest case for RLGC, because under the design each Ractor owns a separate heap and a confined local GC **must not** read or free objects in another objspace, yet a copied payload is by construction a fresh *unshareable* object that briefly lives in one objspace while being referenced from another. The §3.10 (`RACTOR_LOCAL_GC_DESIGN.md` 6.x) **ownership trilemma** is: a copy/move payload `v`

1. is **allocated in the SENDER's objspace** (`ractor_copy`/`ractor_move` run on the sender thread), but
2. is reachable **only from the RECEIVER's basket queue** — a cross-objspace edge that *both* local GCs skip (the sender's local GC never walks a foreign receiver's queue; the receiver's local GC never reads into the sender's objspace), and
3. its sole real keep-alive (the per-page `shared_bits` remset) is **rebuilt from scratch by every global STW GC** purely from shareable→unshareable edges — and an in-flight copy is not such an edge.

So `v` can be freed by the sender's next local GC while still queued (dangling basket → "mark T_NONE"/UAF), and even if pinned, a global GC will drop that pin. This slice resolves the trilemma with two mechanisms: **pin while in flight** (shared-bits remset, re-stamped across the global GC) and **materialize on receive** (re-clone into the receiver's own heap so ownership matches physical location). The new field `rb_ractor_sync::in_flight_materializing` closes the one window the queue walk cannot cover.

### 6.1 Pin the payload in the sender's objspace at send time

`ractor_sync.c:834` (in `ractor_basket_new`)

```c
    if (type == basket_type_copy || type == basket_type_move) {
        rb_gc_pin_in_flight_message(v);
    }
```

**What.** Right after `ractor_prepare_payload` produces the copied/moved payload `v`, mark it in the sender objspace's `shared_bits` remset. `rb_gc_pin_in_flight_message` (`gc.c:3645`) sets the per-page shared bit *atomically* and flags the page `has_shared_objects`:

```c
void rb_gc_pin_in_flight_message(VALUE obj) {
    if (!rlgc_has_local) return;
    if (SPECIAL_CONST_P(obj) || RB_OBJ_SHAREABLE_P(obj)) return;
    MARK_IN_BITMAP_ATOMIC(GET_HEAP_SHARED_BITS(obj), obj); // sender mutator sets concurrently with other Ractors
    GET_HEAP_PAGE(obj)->flags.has_shared_objects = TRUE;
}
```

**Why.** Without this, `v` is reachable only via the (foreign) receiver queue, so the sender's own local GC — which *does* walk the sender objspace — frees it while it is still queued. The shared-bits remset is exactly the mechanism a local GC consults for "unshareable object referenced from outside," so reusing it pins `v` in its home objspace.

**How it fits.** This is the *home-objspace pin* leg. It mirrors the design invariant that unshareable objects referenced across the objspace boundary must be tracked in `shared_bits`. The sender pins its own freshly-created object on its own thread, so there is no cross-objspace race (the atomic set guards only against *other* Ractors writing the same bitmap word — the TSan-discovered non-atomic-RMW class).

**Gotchas.** The pin uses `MARK_IN_BITMAP_ATOMIC`; a plain `|=` would be a lost-update bug under concurrent senders. Note it is keyed on `type` *after* `ractor_prepare_payload` may have *downgraded* `basket_type_copy`→`basket_type_ref` for an already-shareable object (`ractor_sync.c:818`), so a shareable payload correctly skips the pin (and `rb_gc_pin_in_flight_message` early-returns on `RB_OBJ_SHAREABLE_P` anyway).

### 6.2 Re-stamp the queued-basket pin across a global GC

`ractor_sync.c:212` (`ractor_basket_mark`)

```c
    rb_gc_mark(b->p.v);
    /* ... a GLOBAL GC clears every shared bit up front and rebuilds the remset solely from
     * shareable->unshareable edges; an in-flight message ... is not such an edge, so its pin
     * would be lost. ... */
    if (!rb_gc_during_local_gc_p()) {
        rb_gc_pin_in_flight_message(b->p.v);
    }
```

**What.** Every basket still in a receiver's `recv_queue`/port is visited by `ractor_queue_mark`→`ractor_basket_mark`. The mark itself was always there; the new line *re-applies* the in-flight pin — but only when `!rb_gc_during_local_gc_p()`, i.e. only during the global STW GC.

**Why.** Leg 6.1's pin is wiped by the global GC's up-front shared-bits clear and is not regenerated (no shareable→unshareable edge points at `v`). The global GC marks `v` live (the `rb_gc_mark`), but once it finishes and the sender resumes, the sender's *next local GC* sees `v` with no shared bit and reclaims it while it is still queued. Re-stamping during the global STW pass restores the pin so it survives into the post-global world.

**How it fits / why the guard.** `rb_gc_during_local_gc_p()` (`gc.c:3620`) is `rlgc_has_local && !rlgc_global_gc_active` — it is **false exactly during the global GC** (and false in non-RLGC builds, where `rb_gc_pin_in_flight_message` is a no-op). The guard is load-bearing: only the global STW GC may write *another objspace's* (the sender's) bitmap, because every Ractor is stopped so the write races nothing. A *local* GC writing a foreign objspace's bitmap would be a confinement violation, so it is correctly suppressed.

### 6.3 The `in_flight_materializing` slot — covering the de-queued window

`ractor_core.h:30`

```c
    // Ractor-local GC: a copy/move message currently being materialized (cloned) into this Ractor
    // by ractor_basket_accept. It has left the receiver queue, so the queue walk that re-pins
    // in-flight messages across a global GC (ractor_basket_mark) no longer covers it; ractor_sync_mark
    // re-pins this slot instead. 0 when not materializing. (RACTOR_LOCAL_GC_DESIGN.md 6.3)
    VALUE in_flight_materializing;
```

`ractor_sync.c:866` (`ractor_basket_accept`, the receive path)

```c
    ractor_basket_free(b);   // free the basket struct now (never the payload v); safe before a raise

    if ((type == basket_type_copy || type == basket_type_move) &&
        !rb_gc_object_in_current_objspace_p(v)) {
        rb_ractor_t *const cr = GET_RACTOR();
        const VALUE prev = cr->sync.in_flight_materializing;
        cr->sync.in_flight_materializing = v;
        v = ractor_copy(v);          // re-clone into the RECEIVER's objspace (runs on receiver thread)
        cr->sync.in_flight_materializing = prev;
    }
    if (exception) {
        rb_exc_raise(ractor_make_remote_exception(v, sender));
    }
    return v;
```

**What — materialize-on-receive.** When the receiver dequeues a copy/move payload that does **not** already live in its own objspace (`!rb_gc_object_in_current_objspace_p(v)`, `gc.c:3427` — checks only the current objspace's stable page set, no barrier needed), it re-runs `ractor_copy(v)`. Because this runs on the *receiver* thread, the deep clone (`rb_obj_traverse_replace`→`copy_enter`→`ractor_obj_clone`, `ractor.c:2134`) allocates through the receiver's lock-free newobj path, so the resulting graph physically lives in the receiver's heap — making physical location match ownership. The `rb_gc_object_in_current_objspace_p` guard makes this a no-op for self-send and for non-RLGC single-objspace builds (where it is always true).

**What — the slot.** `ractor_copy` *allocates*, so a global GC can fire mid-clone. At that instant `v` has already left the receiver queue (so 6.2's queue walk no longer covers it) but the new clone is not yet complete — `v` is reachable through nothing the GC scans. Publishing `v` into `cr->sync.in_flight_materializing` before the clone, and clearing it after, gives `ractor_sync_mark` a place to mark and re-pin it. The save/restore of `prev` tolerates any re-entry.

**Why both legs.** Legs 6.1/6.2 keep a *queued* message alive; 6.3 keeps an *in-the-act-of-being-received* message alive. Together they cover the payload's entire lifetime from send to fully-materialized ownership.

### 6.4 `ractor_sync_mark`: mark+re-pin the slot, and lock the foreign-mutated queues

`ractor_sync.c:669`

```c
    rb_gc_mark(r->sync.in_flight_materializing);
    if (!rb_gc_during_local_gc_p()) {
        rb_gc_pin_in_flight_message(r->sync.in_flight_materializing);
    }

    const bool sync_lock = rb_gc_during_local_gc_p();
    if (sync_lock) rb_native_mutex_lock(&r->sync.lock);
    {
        if (r->sync.ports) {
            ractor_queue_mark(r->sync.recv_queue);
            st_foreach(r->sync.ports, ractor_mark_ports_i, 0);
        }
        ractor_mark_monitors(r);
    }
    if (sync_lock) rb_native_mutex_unlock(&r->sync.lock);
```

**What (slot).** Mirrors 6.2 exactly for the materialize slot: always `rb_gc_mark` it (a `0`/`Qfalse` slot marks harmlessly), and re-pin in the sender objspace only during the global STW GC. `r` owns this field and writes it only on its own thread, so it is read race-free here (read either by `r`'s own local GC or by a global GC with all Ractors stopped).

**What (lock).** `recv_queue`/`ports`/`monitors` are mutated by **foreign** Ractors — e.g. a sender's `ractor_send_basket` does `ccan_list_add_tail` into `recv_queue` (`ractor_sync.c:1222`) under the target's per-Ractor mutex `r->sync.lock`. A confined *local* GC marks these lock-free and concurrently with those senders, so it could observe a half-spliced list node or a mid-rehash `st_table` and mark a half-linked/uninitialized basket → "mark T_NONE"/SEGV. The new code takes the **same** `r->sync.lock` around the traversal, but only when `sync_lock` (i.e. only during a local GC).

**How it fits / gotchas.**
- The lock is taken via **raw `rb_native_mutex_lock`, not `RACTOR_LOCK`** — `RACTOR_LOCK` additionally sets `malloc_gc_disabled`/`locked_by` bookkeeping that is wrong to touch from inside the GC. This is a deliberate, subtle distinction a reviewer should confirm stays a raw native lock.
- **No self-deadlock:** `RACTOR_LOCK` (used by mutators that hold this lock) sets `malloc_gc_disabled`, so a GC can never trigger while a thread already holds `r->sync.lock` — therefore the GC never re-enters a lock it is already holding.
- During a **global** STW GC the lock is skipped (`sync_lock` false): all Ractors are stopped, no sender runs, and taking it would be pointless (and the global GC already holds the VM barrier).

### 6.5 Initialization

`ractor_sync.c:747` (`ractor_sync_init`)

```c
    r->sync.recv_queue = ractor_queue_new();
    r->sync.in_flight_materializing = 0; // no copy/move message is being materialized yet
```

**What/why.** The new field is zero-initialized so `ractor_sync_mark` marks `0` (a no-op) until an actual `ractor_basket_accept` publishes a payload. Trivial but necessary: without it the field could read uninitialized garbage and `rb_gc_mark` a non-object.

### 6.6 Foreign-Ractor fiber/EC skip (why the receiver re-clones at all)

The materialize-on-receive design is forced by the confinement rule embodied in `rb_gc_local_gc_foreign_ractor_p` (`gc.c:3631`):

```c
bool rb_gc_local_gc_foreign_ractor_p(const rb_ractor_t *owner) {
    if (!rb_gc_during_local_gc_p()) return false;
    return owner != rb_ec_ractor_ptr(rb_gc_get_ec());
}
```

A local GC must not walk a **foreign** Ractor's running execution state (its threads'/fibers' control frames and machine stacks are unstable on another live thread → the read races → SEGV). Consequently a confined GC reaches only its *own* Ractor's roots and never the producer side of a cross-objspace message edge — which is precisely why the payload must be re-homed into the receiver's objspace (6.3) rather than left referenced across the boundary. This is the same confinement principle behind `rb_gc_mark_ractor_local_roots` (`ractor.c`), where the local mark deliberately skips the thread *wrapper* object (`th->self`, which may live foreign in the main objspace) but marks the thread's own in-objspace machine/VM-stack roots directly via `rb_gc_mark_thread_roots`.

**Reviewer gotchas across this slice.**
- Every `rb_gc_pin_in_flight_message` call site is gated either by build/locality (`rlgc_has_local`) inside the helper, or by `!rb_gc_during_local_gc_p()` at the call site — confirm no path lets a *local* GC write a *foreign* objspace's shared bits.
- The `in_flight_materializing` slot must be set **before** the allocating `ractor_copy` and restored **after**; the save/restore of `prev` is what makes nested/re-entrant receives safe — do not "simplify" it to an unconditional clear-to-0.
- `ractor_basket_free(b)` is now called **before** the possible `rb_exc_raise` (the basket struct is freed up front so a raise cannot leak it); it frees only the basket node, never the payload `v`. Confirm no later code dereferences `b`.
- The pin/re-pin is correctness-critical but *conservative*: it only ever keeps an object alive longer; a subsequent global GC recomputes `shared_bits` from real edges and will reclaim `v` once it is genuinely unreferenced, so there is no permanent leak.

---

## 7. Satellite fixes (per-subsystem)

These are the per-subsystem confinement-miss and concurrency fixes that fall out of the RLGC model: a *confined* local GC marks only its own Ractor's roots and foreign-skips everything else, so any object that is **reachable only through a parent/main-objspace container, a VM-global table, or a foreign object's field** is invisible to that local GC and gets swept while live. Each fix below either (a) re-homes such an object into the right objspace, (b) keeps it alive from the local GC, or (c) closes a lock-free read/mutate race. They map to Faces B, D, G, G-2 and the thread/fiber-root marking machinery.

---

### 7.1 `rb_const_remove`: atomic lookup+removal under the VM lock (Face B)

`variable.c:3648`

```c
VALUE
rb_const_remove(VALUE mod, ID id)
{
    VALUE val = Qnil;
    bool not_found = false;
    bool deprecated = false;

    rb_check_frozen(mod);

    RB_VM_LOCKING() {
        rb_const_entry_t *ce = rb_const_lookup(mod, id);
        if (!ce) { not_found = true; }
        else {
            deprecated = RB_CONST_DEPRECATED_P(ce);
            ...
            rb_clear_constant_cache_for_id(id);
            val = ce->value;
            if (UNDEF_P(val)) { autoload_delete(mod, id); val = Qnil; }
            if (ce != const_lookup(RCLASS_PRIME_CONST_TBL(mod), id)) { SIZED_FREE(ce); }
        }
    }

    if (not_found) { ... rb_name_err_raise(...); undefined_constant(...); }
    if (deprecated && rb_warning_category_enabled_p(...)) { rb_category_warn(...); }
    return val;
}
```

**What.** The whole lookup→delete→cache-clear→free sequence is now wrapped in a single `RB_VM_LOCKING()` critical section. The original code did `rb_const_lookup` *outside* any lock, then mutated and freed `ce` unlocked. Two results that previously ran inline are now **deferred until after unlock**: the not-found `rb_name_err_raise`/`undefined_constant`, and the deprecation `rb_category_warn`. They are captured into `not_found` / `deprecated` flags inside the lock and acted on outside it.

**Why.** Constant tables are mutated concurrently from multiple Ractors — `rb_const_set` of a *shareable* value is permitted off the main Ractor and is itself VM-locked (the diff comment says it "Mirrors `rb_const_set()`/`const_tbl_update()`"). An unlocked lookup lets two concurrent removes obtain the **same** `rb_const_entry_t` and double-free it, and lets a remove race a `const_set` writing into freed memory. Worse for cache integrity: `rb_clear_constant_cache_for_id()` walks the per-id inline-cache `set_table` (`set_table_foreach`), which other Ractors *insert into under the lock* on a constant-cache miss; walking it unlocked means a concurrent insert's rehash reallocates `entries[]` out from under the walk, leaving a dangling inline-cache pointer. `autoload_delete()` also mutates the VM-global `autoload_features` hash.

**How it fits.** This is a classic Face-B IC-lifetime fix: the constant-cache `set_table` and the const entry are VM-global state read/written by many Ractors, so all mutation must be serialized by the VM lock, matching the existing write path.

**Reviewer gotchas.**
- The deferral is *required*, not cosmetic: `rb_name_err_raise` and `rb_category_warn` can run Ruby code (raise / call a custom warner), which must not happen while holding `RB_VM_LOCKING()` (re-entrancy / deadlock). Confirm nothing inside the lock can raise before the flags are set.
- Note `val` defaults to `Qnil` and is only meaningful when `!not_found`; the not-found branch never returns normally (it always raises), so the post-lock `return val` is only reached on success.
- The autoload-features hash race itself is **not** closed here (still tracked as an open item: lock-ordering design needed); this fix only takes the same VM lock the rest of the const path uses.

---

### 7.2 `rb_free_generic_ivar`: non-barrier lock during sweep

`variable.c:1339`

```c
/* NON-BARRIER: this runs during a (possibly Ractor-local, lock-free) GC sweep,
 * which is not at a safepoint. A barrier-aware lock here could join a pending
 * global-GC barrier mid-sweep, leaving the objspace half-swept for the global GC to
 * mark. The non-barrier lock still serializes with concurrent table accessors. */
RB_VM_LOCKING_NO_BARRIER() {
    if (!st_delete(generic_fields_tbl_no_ractor_check(), &key, &value)) {
        rb_bug("Object is missing entry in generic_fields_tbl");
    }
}
```

**What.** `RB_VM_LOCKING()` → `RB_VM_LOCKING_NO_BARRIER()` around the `st_delete` from the VM-global `generic_fields_tbl`.

**Why.** This deletion happens during *sweep*, which under RLGC is a lock-free local GC running outside any safepoint. A barrier-aware lock (`RB_VM_LOCKING`) can, on acquisition, *participate in a pending global-GC barrier* — i.e. the sweeping Ractor would stop mid-sweep at the barrier, and the global GC would then mark an objspace that is half-swept (some objects already freed, some not). The no-barrier variant still mutually-excludes concurrent `generic_fields_tbl` accessors but never yields to the barrier.

**How it fits.** Same pattern the RLGC work applies in `gc.c` (`RB_VM_LOCKING_NO_BARRIER()` at gc.c:2144/2201/2425/3668) for any VM-global structure a local GC touches during mark/sweep. The memory note records this as the established rule: a confined GC must serialize with writers but must not block on the barrier.

**Reviewer gotcha.** The correctness rests on `RB_VM_LOCKING_NO_BARRIER` truly *not* joining the barrier; if that macro's semantics ever change, this becomes a mid-sweep STW hazard. The `rb_bug` on a missing entry is unchanged and is the canary if the table is corrupted by a missed lock elsewhere.

---

### 7.3 Symbol & string VM-global table accessors + keep-alive (Face D, part 2)

`internal/symbol.h:21`, `symbol.c:445`, `string.c:572`

```c
/* symbol.c */
VALUE rb_gc_vm_global_symbol_set(void) { return ruby_global_symbols.sym_set; }
VALUE rb_gc_vm_global_symbol_ids(void) { return ruby_global_symbols.ids; }

/* string.c */
VALUE rb_gc_vm_global_fstring_table(void) { return fstring_table_obj; }
```

**What.** Three trivial accessors exposing the VM-global dedup tables: the symbol `str->sym` `concurrent_set` (`sym_set`), the `serial->sym` array (`ids`), and the frozen-string dedup `concurrent_set` (`fstring_table_obj`). The two symbol prototypes are added to `internal/symbol.h`; the fstring one is already declared where `gc.c` can see it.

**Why / how it fits.** These back-stores are **not WB-protected**, and a resize can reallocate the backing into *whichever Ractor crosses the load factor* — i.e. a non-main worker's objspace. The global-roots mark is skipped on the confined path, so without help that worker's local GC would sweep a table other Ractors are concurrently using. `gc.c`'s confined `rb_gc_mark_roots` therefore calls, before returning:

```c
gc_keepalive_vm_global_if_local(id2ref_value);
gc_keepalive_vm_global_if_local(rb_gc_vm_global_fstring_table());
gc_keepalive_vm_global_if_local(rb_gc_vm_global_symbol_set());
gc_keepalive_vm_global_if_local(rb_gc_vm_global_symbol_ids());
```

where `gc_keepalive_vm_global_if_local` marks the object **only if it lives in the current objspace** (`rb_gc_object_in_current_objspace_p`). So each Ractor keeps *its own* copy of a re-homed table alive; a foreign one is left to its owner / the global GC. This is the second half of Face D (the first half being the original VM-global concurrent_set keep-alive).

**Reviewer gotchas.**
- These accessors expose mutable VM-global state by value (a `VALUE`); they are read-only snapshots for the GC and must not be used to *mutate* the table off the owning path.
- The keep-alive is a *coarse* fix: it pins the entire backing array/set object, not individual entries. That is intentional (the entries are pinned separately, see 7.4), but a reviewer should confirm the *contents* are independently rooted — the table object being alive does not by itself keep its unshareable entries alive under a confined GC.

---

### 7.4 Symbol id-entry bucket marked shareable (Face D, part 2)

`symbol.c:281`

```c
rb_darray_make(&entries, ID_ENTRY_UNIT);
id_entry_list = TypedData_Wrap_Struct(0, &sym_id_entry_list_type, entries);
/* ... a worker's confined local GC would not mark it and would sweep it -- yet it holds
 * permanent (immortal-symbol) state shared VM-wide. Mark it shareable so the local-GC sweep
 * pins it; the global GC reclaims it via ids normally. */
RB_OBJ_SET_SHAREABLE(id_entry_list);
rb_ary_store(ids, (long)idx, id_entry_list);
```

**What.** Each newly allocated id-entry bucket (a `TypedData` wrapper holding a `darray` of `ID_ENTRY_UNIT` slots) is flagged `RB_OBJ_SET_SHAREABLE` immediately after creation.

**Why.** The bucket is allocated in the **current Ractor's** objspace (whoever first interns a symbol in that serial range), but it is reached only through `symbols->ids`, which lives in the **main** objspace. A worker's confined local GC marks neither `ids` (foreign) nor, transitively, the bucket — so it would sweep a structure holding permanent immortal-symbol state used VM-wide. The shareable flag makes the **sweep guard** pin it in its home objspace (a local GC never frees a shareable), while the global GC still reclaims it normally via `ids`. The comment notes it only ever holds shareable symbols and frozen strings, so flagging it shareable is sound.

**How it fits.** This is the same shareable-as-sweep-pin trick the comment cross-references for shape edge tables and `rb_managed_id_table_create`. It is the **per-entry** complement to the **whole-table** keep-alive in 7.3: 7.3 keeps `ids`/`sym_set` alive when re-homed; 7.4 keeps each individual bucket alive regardless of which worker allocated it.

**Reviewer gotchas.**
- Correctness depends on the invariant that the bucket *only* ever stores shareable contents (immortal symbols + frozen strings). If a non-shareable VALUE were ever inserted into a shareable-flagged container, that would violate the shareability invariant. Verify `set_id_entry`'s callers only store shareable symbols/fstrings.
- This is on the *allocation* path of every new bucket, not just main-Ractor — that is the point, but it means the flag is set unconditionally even in single-Ractor mode (harmless, just a no-op pin).

---

### 7.5 Thread main-thread container re-home: interrupt queue & mask stack (Face G), `ec->storage` (Face G-2)

`thread.c:687`

```c
if (th->invoke_type == thread_invoke_type_ractor_proc) {
    VALUE q = rb_ary_dup(th->pending_interrupt_queue);
    RBASIC_CLEAR_CLASS(q);
    th->pending_interrupt_queue = q;
    VALUE m = rb_ary_dup(th->pending_interrupt_mask_stack);
    RBASIC_CLEAR_CLASS(m);
    th->pending_interrupt_mask_stack = m;
    /* fiber-storage Hash inherited from the parent by rb_fiber_inherit_storage() */
    if (!NIL_P(th->ec->storage)) {
        th->ec->storage = rb_obj_dup(th->ec->storage);
    }
}
```

**What.** Early in `thread_start_func_2` — i.e. once we are *running inside this Ractor's own objspace* — the new Ractor-main thread re-dups three inherited containers in place: the pending-interrupt queue, the interrupt mask stack (Face G), and the fiber-storage Hash `ec->storage` (Face G-2). Guarded by `invoke_type == thread_invoke_type_ractor_proc` so only genuine Ractor-proc threads pay the cost; a regular thread shares its spawner's objspace and needs nothing.

**Why.** `thread_create_core()` allocates `pending_interrupt_queue` / `pending_interrupt_mask_stack` on the **spawning (parent) Ractor's** thread, *before this Ractor's objspace exists* (it is created by `rb_ractor_living_threads_insert`). So the containers physically live in the **parent** objspace. The new thread then stores its **own** (this-objspace, non-shareable) objects into them — `Thread.handle_interrupt` pushes mask hashes onto the mask stack; `Fiber[]=` / `storage=` write into the inherited fiber-storage Hash. A confined local GC of *this* Ractor foreign-skips a parent-objspace container and therefore never reaches the this-objspace objects reachable only through it → they are swept while live → UAF on interrupt delivery or `Fiber[]` read. Re-dup'ing here, in this objspace, relocates the container so the local GC owns and marks it, while `rb_ary_dup`/`rb_obj_dup` preserve the inherited contents.

**How it fits.** This is the canonical Family-I confinement-miss cure by *re-homing*: move the container into the objspace whose GC must mark its contents, rather than trying to teach the local GC to chase a foreign pointer.

**Reviewer gotchas.**
- `RBASIC_CLEAR_CLASS(q)` / `RBASIC_CLEAR_CLASS(m)` is deliberate: these are internal arrays that must remain class-less (as the originals were); `rb_ary_dup` would otherwise carry the class. Confirm the originals were also class-less so behavior is identical.
- `ec->storage` uses `rb_obj_dup` (a Hash) and is guarded by `!NIL_P` — the storage Hash may be nil if no fiber storage was inherited (`rb_fiber_inherit_storage`). Watch that this is a **shallow** dup: the *contents* of the storage Hash are still whatever was inherited; correctness relies on those contents being shareable (inherited storage is shareable) so they need no re-home.
- Timing matters: this runs *after* the `RB_VM_UNLOCK()` block above it but before the thread does any user work, so no `handle_interrupt`/`Fiber[]=` can have populated the *old* containers yet — the dup captures the pristine inherited state. If this block ever moves later, it could lose writes.
- The memory note flags a *related* still-open case: `thread_variable_set`'s locals Hash is an ivar of the parent-objspace `Thread` (`th->self`) and is **not** covered here — reviewers should not assume all thread-local containers are re-homed by this block.

---

### 7.6 `vm.c`: confinement assertion + strong gen-fields cache root in EC mark

`vm.c:3700` and `vm.c:3768`

```c
/* Ractor-local GC invariant: a local GC must only ever walk an EC belonging to its OWN Ractor. */
VM_ASSERT(ec->thread_ptr == NULL || !rb_gc_local_gc_foreign_ractor_p(ec->thread_ptr->ractor));
...
rb_gc_mark_movable(ec->gen_fields_cache.obj);
rb_gc_mark_movable(ec->gen_fields_cache.fields_obj);
```

**What.** Two additions to `rb_execution_context_mark`. First, an assertion that the EC being marked is **not** a foreign (concurrently-running) Ractor's EC — `rb_gc_local_gc_foreign_ractor_p` returns true during a local GC iff the owner is some other Ractor. Second, the generic-ivar fast-path cache slots `{obj, fields_obj}` are now marked as **strong movable roots**.

**Why (assertion).** Walking a foreign EC races on its live control frames and machine stack → SEGV. Callers (`cont_mark`, the per-Ractor root path) are *supposed* to skip foreign ECs; this `VM_ASSERT` catches any path that forgets. The `ec->thread_ptr == NULL` short-circuit covers ECs not yet attached to a thread.

**Why (gen-fields cache).** The cached `fields_obj` (an `imemo_fields`) is referenced *only* from this EC and from the `generic_fields_tbl`. It is genuinely a **strong** reference, not weak: if it were not marked, a sweep could free it while the live EC still holds it, and a later `rb_mark_generic_ivar` would mark a freed (T_NONE) `imemo_fields`. The diff comment records this as a flaky crash that appeared once concurrent lock-free local GCs run: the *live* EC's cache was never weak-cleared (only cont/fiber *saved* ecs are). Cost is at most one extra GC cycle of float, freed when the cache is next overwritten; the compaction pass already treats these slots as movable, so `rb_gc_mark_movable` is the matching marker.

**How it fits.** Family III / generic-ivar correctness: the gen-fields cache is part of the EC's own roots, so it belongs in the EC mark and is reached by both the confined local path (own EC) and the global path.

**Reviewer gotchas.**
- The two cache slots must be marked **movable** (not pinned) to stay consistent with the compaction update of `ec->gen_fields_cache` higher in the same function; pinning them would desync compaction. Note compaction is disabled under RLGC, so the movable marking is the conservative/forward-compatible choice.
- The assertion is `VM_ASSERT` (debug builds only). In release builds a forgotten foreign-skip becomes a latent race, not a crash — so the *callers'* foreign guards remain load-bearing.

---

### 7.7 `vm.c`: `rb_gc_mark_thread_roots` — direct thread/root-fiber marking for confined GC

`vm.c:3851`

```c
void
rb_gc_mark_thread_roots(rb_thread_t *th)
{
    thread_mark((void *)th);
    if (th->ec) rb_execution_context_mark(th->ec);

    if (th->ec && th->root_fiber && th->root_fiber != th->ec->fiber_ptr) {
        rb_gc_mark_fiber_saved_context(th->root_fiber);
    }
}
```

**What.** A new entry point (declared in `ractor.c`, called from `ractor.c:286`) that marks a thread's roots **directly**, plus a forward decl `void rb_gc_mark_fiber_saved_context(rb_fiber_t *fib);` (defined in `cont.c:1286` as a thin wrapper over `cont_mark(&fiber->cont)`).

**Why.** This closes a two-layer confinement miss for Ractor threads/fibers whose **wrapper objects live in the main/parent objspace**:

1. The `rb_thread_t`'s wrapper `th->self` may live in the main objspace — foreign to this Ractor's local objspace — so the local mark skips `th->self` and `thread_mark` would never run to reach the thread's roots that *do* live in this objspace. Calling `thread_mark((void*)th)` directly fixes that.
2. `thread_mark` reaches the EC's VM stack only through the **fiber wrapper** (`rb_fiber_mark_self`); that wrapper may also be foreign and skipped. So we additionally mark the running `th->ec` directly — that stack holds the Ractor's live local variables.
3. The **root fiber's** wrapper is created on the *parent* thread during `Ractor.new`, so it lives in the parent (e.g. main) objspace and is foreign-skipped. When a *non-root* fiber is running, the root fiber's *saved* stack — which holds this Ractor's top-level locals and re-roots its whole fiber graph — would be left unmarked, so a local GC would free objects a later global GC then marks ("mark T_NONE", parent fiber/Fiber). `rb_gc_mark_fiber_saved_context(th->root_fiber)` marks that suspended saved context directly.

**How it fits.** This is the thread/fiber half of Face G's broader confinement story and is the linchpin against the "mark T_NONE parent fiber" crash family in the memory notes. It mirrors the `cont.c:1161` foreign-guard logic on the saved-EC side.

**Reviewer gotchas.**
- The `th->root_fiber != th->ec->fiber_ptr` guard is essential: if the root fiber *is* the running fiber, its **saved** context is *stale* (the live state is in `th->ec`, already marked via item 2). Marking a stale saved context could mark garbage or double-cover; the guard skips exactly that case. Reviewers should confirm `rb_gc_mark_fiber_saved_context` is only ever passed a genuinely **suspended** fiber — the `cont.c` comment makes this a documented precondition.
- `cont_mark` retains its own owner-based foreign guard (`cont.c:1161`: `rb_gc_local_gc_foreign_ractor_p(...)`), so passing a genuinely foreign fiber here is still skipped defensively — but the *intended* contract is that the caller (`ractor.c`) only calls this for this Ractor's own threads.
- This function calls `rb_execution_context_mark(th->ec)`, which now contains the 7.6 `VM_ASSERT` — so an accidental foreign call will trip that assertion in debug builds, which is the intended safety net.

---

### 7.8 `iseq.c`: VM-global iseq/callcache sweeps walk *all* objspaces

`iseq.c:4266`, `iseq.c:4287`, `iseq.c:4318`

```c
-        rb_objspace_each_objects(clear_attr_ccs_i, NULL);
+        rb_objspace_each_objects_all_ractors(clear_attr_ccs_i, NULL);
...
-        rb_objspace_each_objects(clear_bf_ccs_i, NULL);
+        rb_objspace_each_objects_all_ractors(clear_bf_ccs_i, NULL);
...
-        rb_objspace_each_objects(trace_set_i, &turnon_events);
+        rb_objspace_each_objects_all_ractors(trace_set_i, &turnon_events);
```

**What.** Three VM-global iseq/callcache traversals — `rb_clear_attr_ccs`, `rb_clear_bf_ccs`, `rb_iseq_trace_set_all` — switch from `rb_objspace_each_objects` (caller's objspace only) to the new `rb_objspace_each_objects_all_ractors` (gc.c:4170), which iterates the main objspace, every live Ractor's `local_gc_objspace`, **and every orphaned objspace** on the orphan list.

**Why.** These are VM-global operations that must reach **every heap-resident iseq/callcache regardless of which Ractor allocated it** — e.g. enabling a TracePoint must patch *all* iseqs (an iseq compiled in a worker Ractor still needs its trace bytecode set), and clearing attr/bf callcaches must invalidate caches in every objspace. Under RLGC, iseqs and callcaches are scattered across per-Ractor objspaces, so the single-objspace walk would silently miss those allocated by other Ractors — a TracePoint that doesn't fire, or a stale callcache.

**How it fits.** Each of these three sites already holds the VM barrier (`rb_vm_barrier()` inside `RB_VM_LOCKING()`, or `ASSERT_vm_locking_with_barrier()` for `rb_clear_bf_ccs`) — which is exactly the precondition `rb_objspace_each_objects_all_ractors` documents ("The caller MUST hold the VM barrier so the object sets are stable"). This is the global/STW side of the model: with all Ractors stopped, walking every objspace (including orphaned ones) is safe.

**Reviewer gotchas.**
- The new helper's safety hinges entirely on the **barrier precondition**. `rb_clear_bf_ccs` does *not* take the lock itself — it asserts `ASSERT_vm_locking_with_barrier()`, so the caller must establish it. If any future caller invokes one of these without the barrier, the per-Ractor object sets are unstable and the walk can crash. Verify all callers.
- The orphaned-objspace inclusion is deliberate and important: an orphaned objspace from a terminated Ractor can still hold live shareable iseqs/callcaches referenced from elsewhere, so skipping it would re-introduce the orphaned-objspace family of bugs. Confirm the orphan-list walk in the helper has no concurrent mutator (it relies on the same STW guarantee).

---

*この副読本は実 diff (`git diff de5545202 HEAD`) を読みながら参照してください。残課題・現状サマリは `RLGC_STATUS.md`、設計の経緯は `RACTOR_LOCAL_GC_DESIGN.md` §6.x。*
