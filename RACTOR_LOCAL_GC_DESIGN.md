# Ractor-local GC 設計ドキュメント

CRuby の GC を **Ractor ごとに独立した objspace** へ分割し、各 Ractor が自分のヒープを
**Stop-The-World なしで並行に** mark/sweep する実装。`gc/default/default.c` のみ対象
(mmtk/wbcheck は対象外)。本ドキュメントだけで第三者が同等実装を再現できることを狙う。

ステータス: 実験実装、**未コミット**。`make test-all` env-on **34849 / 0 failures**、
env-off 0 failures、`make btest` 2050/2050、bench.rb N=8 で **~4.7 実効コア**。

---

## 1. 一般的なデザイン

### 1.1 中心アイデア
- **per-Ractor objspace**: 非 main Ractor は各自 `rb_objspace_t` を持つ。`rb_gc_get_objspace()`
  は「現在の Ractor の objspace」を返す。main Ractor の objspace = VM 全体の objspace。
- **3 種類の GC**:
  1. **ローカル minor / major GC** — その Ractor の objspace だけを mark/sweep。VM ロックも
     バリアも取らず、他 Ractor の実行・他 Ractor のローカル GC と**並行**に走る。
  2. **グローバル GC** — full/major のとき全 Ractor を STW バリアで止め、**全 objspace を
     統一 mark/sweep**。shareable を「到達性」で回収する唯一の機会。
- **ロックフリー割り当て** — ローカル objspace は Ractor 専有なので `newobj` は VM ロックを
  取らない(これが元々のスケーリング・ボトルネックだった)。

### 1.2 不変条件 (これを守れば正しい)
1. **objspace 解決はアラインメント**: ヒープページは 64KiB (`HEAP_PAGE_ALIGN`) アライン。
   任意のポインタ `p` から `GET_HEAP_PAGE(p)->objspace` で所有 objspace が分かる
   (`page->objspace` 後方ポインタ)。**ただし精密マークのみ**。保守的スキャン(任意ワード)は
   アラインしたページ本体を**デリファレンスしてはならない**(ページは個別 mmap でギャップが
   unmap されていて SEGV する) → `rb_gc_conservative_owner()`(各 objspace の安全な
   sorted-array bsearch)を使う。
2. **封じ込め (confinement)**: ローカル GC の `gc_mark` は
   `GET_HEAP_OBJSPACE(obj) != objspace` のオブジェクトを**辿らない**(live leaf 扱い)。
   他 objspace のオブジェクトはその所有者の GC が生かす。
3. **shareable のピン**: ローカル GC は「他 objspace から参照されているか」を判定できないため
   **shareable を絶対に解放しない**(pin)。回収はグローバル GC のみ。
4. **shared_bits remset**: 「shareable から**直接**参照される unshareable 境界オブジェクト」を
   per-page ビットマップで記録。これがローカル GC のルートになる(shareable 親が別 objspace に
   居ても、その境界の子を所有者のローカル GC が生かせる)。WB で維持、グローバル full mark で
   全クリア+再計算。境界の子の subtree は通常の `local` オブジェクトなのでローカル回収可能。
   ソundsness: s→u エッジは常に **u の所有者**が作る(隔離則: 他 Ractor の unshareable 参照は
   持てない)ので、自分のオブジェクト u にビットを立てれば良い(親 s は触らない)。
5. **VM 内部キャッシュは main へ**: method/inline cache 等は VM ロック保持中に生成される。これらは
   shareable で cross-Ractor に共有されるので、ローカル GC が回収しないよう「born-shareable は
   shared_bits」+ 「sweep で cc/cme/callinfo の imemo は常にピン」。
6. **共有可変構造へのアクセスは同期**: 並行ローカル GC が触らざるを得ない VM グローバル/Ractor 固有の
   可変構造は **NON_BARRIER ロック**(VM ロックだがグローバル GC バリアに途中参加しない版)等で同期
   (§3.7)。

### 1.3 性能の考え方
- 元のボトルネック: `newobj_cache_miss` が `RB_GC_CR_LOCK()`(プロセス全体で1個の
  `vm->ractor.sync.lock`)を**毎キャッシュミス**で取得 → 全 Ractor の割り当てとローカル GC が
  直列化。`perf` で ctx-switch の 98% が `rb_native_mutex_lock` と判明 (2.6 実効コア)。
- 対策: ローカル objspace ではこのロックを除去(フルロックフリー)。→ 4.7 実効コア。
- 二次対策: ページ本体の per-page `mmap`+`munmap`(64KiB アライン用)が kernel の process-wide
  `mmap_lock` を直列化 → **per-objspace アリーナ**(2MiB アライン・THP・no-trim-munmap)。

### 1.4 ランタイムトグル (env)
- `RUBY_RACTOR_LOCAL_GC=1` — per-Ractor objspace 機能全体を有効化(既定 OFF、つまり既定は従来通り)。
- `RUBY_RACTOR_GLOBAL_GC=0` — グローバル GC を無効化(major も confined ローカルになり shareable は
  objspace 寿命まで pin/leak)。既定 ON。
- `RUBY_RACTOR_LOCAL_GC_LOCKFREE=0` — ロックフリー割り当てを無効化。既定 ON。
- `RLGC_STATS=1` — 終了時にローカル GC 回数 / 最大同時実行数 / グローバル GC 回数を表示。
- `RLGC_DEBUG` / `RACTOR_LOCAL_GC_AUDIT`(コンパイル時 1) — WB 完全性監査。

---

## 2. 変更点一覧 (master との diff)

```
 gc.c                 | 272 +    VM 側グルー: objspace ルーティング, conservative_owner,
                       |          全 objspace 走査, generic-ivar/id2ref 同期, ローカルルート
 gc/default/default.c | 849 +    GC 本体: per-objspace objspace, confined mark, shared_bits,
                       |          global GC, lock-free alloc + arena, 並行 race 修正
 id_table.c           |   5 +    managed_id_table_dup に RB_OBJ_SET_SHAREABLE (グローバル GC 根本修正)
 internal/gc.h        |  20 +    新 API の宣言
 iseq.c               |   6 +    TracePoint/cc クリアを全 objspace に
 ractor.c             |  38 +    per-Ractor objspace 生成, ローカルルートマーク, cache_free(r)
 ractor_core.h        |   9 +    rb_ractor_t に local_gc_objspace / main_newobj_cache
 ractor_sync.c        |  30 +    メッセージポートのマークを per-Ractor ロック, in-flight pin
 variable.c           |   6 +    generic_fields_tbl sweep-delete を NON_BARRIER
 vm.c                 |  26 +    gen_fields_cache を強ルート化, rb_gc_mark_thread_roots
 10 files, +1196 -65
```

機能対応:
- **per-Ractor objspace ライフサイクル**: ractor.c, gc.c, ractor_core.h, default.c(objspace_init)
- **confined mark / 封じ込め**: default.c(gc_mark guard, mark_roots), gc.c(rb_gc_mark_roots local 枝)
- **shared_bits + WB**: default.c(writebarrier, gc_shared_relation, gc_mark_shared_roots, newobj_init)
- **global GC**: default.c(gc_start, gc_global_sweep, gc_enter/exit, gc_marks_start clear), gc.c(foreach_objspace, conservative_owner)
- **lock-free alloc + arena**: default.c(newobj_cache_miss, rlgc_page_arena_*, rlgc_span_extend)
- **並行 GC race 修正(8件)**: §3.7 参照
- **根本バグ修正(クラッシュ)**: id_table.c(dup shareable), vm.c(thread roots), default.c(copy_finalizer)
- **全 objspace 走査**: gc.c(each_objects_all_ractors), iseq.c

---

## 3. 変更点詳細

### 3.1 per-Ractor objspace ライフサイクル

**`rb_ractor_t` に2フィールド追加** (ractor_core.h):
```c
void *local_gc_objspace;   /* この Ractor の objspace。main は VM の objspace を別名参照 */
void *main_newobj_cache;   /* main objspace への割り当て用 (VM 内部構造用、未使用枠) */
```

**生成** (ractor.c `vm_insert_ractor0`): 非 main Ractor 作成時に
`r->local_gc_objspace = rb_gc_objspace_alloc_local()`。main は
`rb_ractor_main_alloc` で `r->local_gc_objspace = GET_VM()->gc.objspace`。

**ルーティング** (gc.c `rb_gc_get_objspace`):
```c
rb_ractor_t *cr = rb_current_ractor_raw(false);
if (cr != NULL && cr->local_gc_objspace != NULL) return cr->local_gc_objspace;
return GET_VM()->gc.objspace;   /* boot 中 / Ractor 無しスレッドのフォールバック */
```

**objspace 生成/解放** (gc.c): `rb_gc_objspace_alloc_local()` =
`rb_gc_impl_objspace_alloc` + `_init` + `_stress_set`。`rb_gc_objspace_free_local`。

**newobj cache は所有 objspace に紐づく** (gc.c `rb_gc_ractor_cache_alloc/free`):
cache はその Ractor の objspace からページを引く。`rb_gc_ractor_cache_free(rb_ractor_t *r)`
は **r->local_gc_objspace に対して**解放(freelist のスロットが正しいヒープに戻る)。
→ シグネチャを `(void *cache)` から `(rb_ractor_t *r)` に変更(ractor.c 全呼び出し更新)。

**objspace 初期化の特別処理** (default.c `rb_gc_impl_objspace_init`):
```c
if (rlgc_main_objspace == NULL) rlgc_main_objspace = objspace;       /* 最初 = main */
else {
    objspace->local = TRUE;
    objspace->flags.dont_incremental = TRUE;  /* ローカル GC は自己完結 stop-mark-sweep */
    if (!rlgc_has_local) {
        gc_rest(rlgc_main_objspace);                 /* in-flight な main GC を終わらせる */
        rlgc_main_objspace->flags.dont_incremental = TRUE;  /* main もアトミックに */
    }
    rlgc_has_local = true;
}
```
理由: 最初の per-Ractor objspace が出来た瞬間から「どの objspace の major もグローバル STW GC」に
なる。グローバル GC のバリアが main を**コレクション途中で**捕まえると統一 mark/sweep が破綻するので、
main も incremental/lazy を切ってアトミックにする。

### 3.2 confined mark (封じ込め)

**`gc_mark` の入口ガード** (default.c):
```c
if (objspace->flags.local_gc && GET_HEAP_OBJSPACE(obj) != objspace) {
    return;   /* 他 objspace のオブジェクトは辿らない(その所有者が生かす) */
}
```

**ローカルルートマーク** (gc.c `rb_gc_mark_roots` 冒頭、`objspace != vm->gc.objspace && !rlgc_global_gc_active` のとき):
- `rb_gc_mark_ractor_local_roots(cr)` — Ractor 自身の内部状態(受信キュー/local storage/std IO/スレッド)。
- `mark_current_machine_context(ec)` — GC を起こしたスレッドの保守的マシンスタック。
- VM グローバルルート(`rb_vm_mark` 等)は**マークしない** → `return`。それらは main objspace に居て
  main/グローバル GC が生かす。

**ローカルルートの肝** (ractor.c `rb_gc_mark_ractor_local_roots`, vm.c `rb_gc_mark_thread_roots`):
```c
void rb_gc_mark_ractor_local_roots(rb_ractor_t *r) {
    ractor_mark((void *)r);
    /* ractor_mark は th->self を辿るが、その wrapper は main objspace に居て confined mark に
     * skip される。VM スタックを持つ ec を直接マークする(これが live local を持つ)。*/
    if (r->threads.cnt > 0) {
        rb_thread_t *th = NULL;
        ccan_list_for_each(&r->threads.set, th, lt_node) rb_gc_mark_thread_roots(th);
    }
}
void rb_gc_mark_thread_roots(rb_thread_t *th) {
    thread_mark((void *)th);
    if (th->ec) rb_execution_context_mark(th->ec);  /* fiber wrapper を迂回して VM スタックを直接 */
}
```
**これが「object-heavy なローカル Ractor が全部クラッシュ」していた根本原因の修正**:
VM スタック上の live local が fiber wrapper(main objspace, foreign)経由でしか辿れず、confined mark が
skip して use-after-free していた。

### 3.3 shared_bits remset + write barrier

**データ構造** (default.c):
- `heap_page::shared_bits[HEAP_PAGE_BITMAP_LIMIT]` + `flags.has_shared_objects`。
- `GET_HEAP_SHARED_BITS(x)`、`GET_HEAP_OBJSPACE(x)`、`page->objspace` 後方ポインタ。

**WB** (`rb_gc_impl_writebarrier(a, b)`): 通常の世代別 WB の**前**に:
```c
if (RB_OBJ_SHAREABLE_P(b)) {                 /* b 自身が shareable → 所有者がピン */
    MARK_IN_BITMAP(GET_HEAP_SHARED_BITS(b), b); GET_HEAP_PAGE(b)->flags.has_shared_objects = TRUE;
}
else if (RB_OBJ_SHAREABLE_P(a) || MARKED_IN_BITMAP(GET_HEAP_SHARED_BITS(a), a)) {
    MARK_IN_BITMAP(GET_HEAP_SHARED_BITS(b), b); ...  /* shareable(or shared) → unshareable 境界 */
}
```

**born-shareable** (`newobj_init`): `flags & FL_SHAREABLE` なら生成時に shared_bits をセット。

**ルートパス** (`mark_roots` → `gc_mark_shared_roots`): `has_shared_objects` なページの
shared_bits を走査し各境界オブジェクトを `gc_mark`(subtree も辿る)。

**full mark 中の再計算** (`gc_mark`→`gc_shared_relation`): エッジ親 `rgengc.parent_object` が
shareable で子が unshareable なら shared_bits を立て直す(AUDIT モードは WB 漏れを報告)。

**クリア規則** (`rgengc_mark_and_rememberset_clear`): `!rlgc_has_local || rlgc_global_gc_active`
のときのみ shared_bits クリア。**ローカル GC はクリアしない**(foreign 親が見えないから)。

**移動時** (`gc_move`): shared_bit を src→dest へ移送。

### 3.4 global GC

**判定** (`gc_start`, gc_enter の前): `rlgc_has_local && rlgc_global_gc_enabled()` かつ
full mark になるなら `objspace->flags.global_gc = TRUE`。minor は confined ローカルのまま。

**gc_enter / gc_exit** (default.c):
```c
if (objspace->local && !objspace->flags.global_gc) {  /* ローカル minor */
    *lock_lev = 0; objspace->flags.local_gc = TRUE;    /* VM ロックもバリアも取らない */
    /* + 同時実行カウンタ(RLGC_STATS) */
} else {
    *lock_lev = RB_GC_VM_LOCK(); rb_gc_vm_barrier();    /* グローバル/main: STW */
}
```

**駆動** (`gc_start`): `rlgc_global_gc_active = flags.global_gc;` → `gc_marks` は VM 全ルート +
unconfined トレース(全 Ractor 停止済み)。完了後 `gc_global_sweep(objspace)` =
`rb_gc_foreach_objspace(gc_global_sweep_one)` で**全 objspace を sweep**(dead shareable 回収)。

**clear の全 objspace 化** (`gc_marks_start`): グローバル時は
`rb_gc_foreach_objspace(gc_full_mark_clear_thunk)` で全 objspace の mark/old/remembered/shared
ビットを先にクリア。

**保守的マーク** (`rb_gc_impl_mark_maybe`): グローバル時は `rb_gc_conservative_owner()` で全 objspace
横断の安全な所属判定(ワードをデリファレンスしない) → DRIVER objspace 経由で pin(mark ビットは
オブジェクト自身のページに付く)。

**sweep の pin ガード** (`gc_sweep_plane`):
```c
if (objspace->local && RB_OBJ_SHAREABLE_P(vp) && !rlgc_global_gc_active) break; /* ローカルは shareable 不解放 */
if (rlgc_has_local && RB_OBJ_SHAREABLE_P(vp) && T_IMEMO &&
    (callcache|callinfo|ment)) break;  /* cc/cme/ci は常にピン(弱インラインキャッシュ + cross-objspace 競合) */
```

### 3.5 lock-free allocation

**`newobj_cache_miss`** (default.c): ローカル objspace かつ lockfree 有効なら **VM ロックを取らない**。
```c
if (!vm_locked && !(objspace->local && rlgc_lockfree_alloc_enabled())) {
    lev = RB_GC_CR_LOCK(); unlock_vm = true;   /* main objspace のみロック */
}
```
ローカル objspace は Ractor 専有 → 空きページ確保もローカル GC も自スレッドのみ(Ractor GVL で直列)+
グローバル GC はバリアで先に止める、ので VM グローバルロック不要。`newobj_slowpath`(stress/unprotected)は
稀なので常にロック。

### 3.6 per-objspace arena allocator

**目的**: per-page `mmap`(128KiB)+2 `munmap`(アライン整形)が process-wide `mmap_lock`(write) を
直列化 + TLB shootdown。

**実装** (default.c, `RACTOR_LOCAL_GC && HAVE_MMAP`):
- `struct rlgc_page_arena { char *mmap_base; size_t mmap_size; next; }`。
  `RLGC_PAGE_ARENA_BODIES 256`(=16MiB)、`RLGC_ARENA_ALIGN 2MiB`。
- `rlgc_page_arena_grow`: `mmap(arena_size + 2MiB)` し 2MiB アラインへ切り上げ、**slack は munmap せず**
  (munmap が mmap_lock を直列化するため放置=物理メモリ無し)、`madvise(MADV_HUGEPAGE)`(64KiB 本体の
  16 回 fault → 2MiB で 1 回へ)。
- `heap_page_body_allocate(objspace)`: freelist pop、無ければ cursor を bump、足りなければ grow。
- `heap_page_body_free(objspace, body)`: arena freelist に push(本体メモリ自身に next を格納)。
- `rlgc_page_arenas_free(objspace)`: objspace 解放時に全 arena を munmap。
- VM グローバル span `[rlgc_global_lomem, rlgc_global_himem)` を `rlgc_span_extend`(atomic CAS min/max)で
  更新(`rlgc_obj_in_any_heap` のレンジチェック用)。

### 3.7 並行ローカル GC が共有データを触る競合の修正(計8件)

ローカル GC が並行実行されると、VM グローバル/Ractor 固有の可変構造を他 Ractor の mutator/GC と同時に
触る。根本パターン: **無同期** or **barrier-aware ロック**(`RB_VM_LOCKING` は保留中グローバル GC バリアに
途中参加 → objspace を中途半端な状態でグローバル GC に渡す)or **VM グローバル GC スクラッチの読み**は競合する。
鍵: グローバル GC バリア発行側(`rb_ractor_sched_barrier_start`)は待機前に VM mutex を**解放**するので、
ローカル GC が **NON_BARRIER** で VM ロックを取ってもデッドロックしない(相互排除だけ得て、バリアには
後で本物のセーフポイントで参加)。`RACTOR_LOCK` は `malloc_gc_disabled` を立てるので、ロック保持中に
GC は起きない(自己デッドロック無し)。

| # | 構造 | 競合 | 修正 |
|---|------|------|------|
| 1 | `generic_fields_tbl_` (VM グローバル st_table) | ローカル GC の mark lookup / sweep delete が writer の rehash と競合 | **NON_BARRIER VM ロック** (gc.c `gc_mark_generic_ivar_sync`, variable.c `rb_free_generic_ivar`) |
| 2 | `id2ref_tbl` (VM グローバル) | sweep delete が無同期 | **NON_BARRIER VM ロック** (gc.c `obj_free_object_id`) |
| 3 | Ractor ポート `recv_queue`/`ports`/`monitors` | foreign sender が per-Ractor mutex `r->sync.lock` で変更、ローカル GC が無ロックで走査 | **生 `rb_native_mutex_lock(&r->sync.lock)`**(RACTOR_LOCK ではなく)を `rb_gc_during_confined_local_gc_p()` のとき取得 (ractor_sync.c `ractor_sync_mark`) |
| 4 | in-flight メッセージコピー(cross-objspace) | 送信側 objspace に居るが受信側 basket からのみ参照 → 両 confined GC が skip → 解放 | 送信側で **shared_bits pin** (gc.c `rb_gc_pin_in_flight_message`, ractor_sync.c `ractor_basket_new`) |
| 5 | `vm->gc.mark_func_data` (VM グローバル GC スクラッチ) | S の `reachable_objects_from`/shareability チェックが設定 → R の実 GC のマークを乗っ取り → "allocation during GC" | 実 GC は `during_gc` で判定し無視 (gc.c `RB_GC_MARK_OR_TRAVERSE`: `mark_func_data==NULL \|\| rb_gc_impl_during_gc_p(objspace)`) |
| 6 | per-EC `gen_fields_cache` | weak 参照で sweep に解放され得る | **強 movable ルート化** (vm.c `rb_execution_context_mark`) |
| 7 | `freed_ractor_local_keys` (VM グローバル配列) | `rb_ractor_finish_marking` が毎 `gc_marks_finish` で free+clear → 並行ローカル GC で二重 free | STW のみ実行 (default.c: `if (!objspace->local \|\| objspace->flags.global_gc)`) |
| 8 | `vm->global_hooks` (VM グローバルフックリスト) | ローカル GC が無ロック走査、writer `hook_list_connect` も無ロック | ローカルルート枝から**除去**(user hook は ractor-local `r->pub.hooks`=`ractor_mark` が、global_hooks は STW グローバル GC が担当) (gc.c) |

#5 のマクロ(最重要):
```c
#define RB_GC_MARK_OR_TRAVERSE(func, obj_or_ptr, obj, check_obj) do { \
    if (!RB_SPECIAL_CONST_P(obj)) { \
        rb_vm_t *vm = GET_VM(); void *objspace = rb_gc_get_objspace(); \
        if (LIKELY(vm->gc.mark_func_data == NULL) || rb_gc_impl_during_gc_p(objspace)) { \
            GC_ASSERT(rb_gc_impl_during_gc_p(objspace)); (func)(objspace, (obj_or_ptr)); \
        } else { /* reachable_objects_from コールバックへリダイレクト(非 GC 文脈のみ) */ ... } \
    } \
} while (0)
```

### 3.8 根本バグ修正(クラッシュ駆動で発見)

- **id_table.c `rb_managed_id_table_dup`**: `RB_OBJ_SET_SHAREABLE(obj)` 追加。dup された
  shape-tree edge テーブルが unshareable のままだとローカル GC が回収 → グローバル GC の
  `shape_tree_mark` が T_NONE。**これがグローバル GC を default-on にできた根本修正**。
- **default.c `rb_gc_impl_copy_finalizer`**: obj と dest が別 objspace のとき、
  `GET_HEAP_OBJSPACE(obj/dest)` の finalizer_table を使う(`rlgc_finalizer_table` アクセサ)。
- **vm.c thread roots / §3.2**: VM スタックを直接マーク。

### 3.9 全 objspace 走査
`rb_objspace_each_objects_all_ractors`(gc.c, バリア保持下で全 objspace を `each_objects`)を新設し、
iseq.c の TracePoint 有効化 / cc クリア(`rb_iseq_trace_set_all` / `rb_clear_attr_ccs` / `rb_clear_bf_ccs`)が
全 Ractor の iseq を対象にするよう変更(従来は現 Ractor の objspace のみ → 他 Ractor の iseq に
trace 命令が入らずイベントが発火しなかった)。

---

## 4. 成果

### 4.1 ベンチマーク (bench.rb: N Ractor が各自 alloc-heavy ループ、wall-clock)
ワークロード: `300 回 { a=[]; 3000.times{ a << [i, "s#{i}", {k=>i}] }; a.clear }`(配列/文字列/ハッシュ生成)。
AMD Ryzen 9 5900HX (8 物理/16 HT)、DDR4-3200 dual-channel。

| 構成 | N=1 | N=8 | N=8 実効コア | CPU% |
|------|-----|-----|------|------|
| env-off (共有 objspace・直列) | 0.199 | 1.568 | **1.0** | 244% |
| RLGC 修正前 (cache-miss ロックあり) | 0.186 | 0.562 | 2.6 | 362% |
| **RLGC フルロックフリー (本実装)** | 0.189 | **0.330** | **~4.7** | 658% |

- 直列ベースライン比 **4.75倍高速** (N=8)。
- `RLGC_STATS`: `local GCs: 431 (max 8 ran concurrently), global GCs: 8`
  → **最大8個のローカル GC が同時実行**(真の GC 並列性を実証)。
- スケーリングの解析: IPC 2.66 維持(メモリ律速でない)、cache-miss/命令数とも線形。
  残る ~8→4.7 のギャップは (a) グローバル GC の STW バリア(~13%)、(b) GC 自体の CPU コスト。

### 4.2 正しさ
- `make test-all` env-on (RLGC + フルロックフリー + グローバル GC、すべて default-on):
  **34849 tests, 0 failures, 0 errors, 149 skips**。
- `make test-all` env-off: **34837 tests, 0 failures**(非 RLGC 経路に回帰なし)。
- `make btest`: フル **2050/2050**(複数回)、`test_ractor.rb` 単体多数回安定。
- [Bug#18117] ポート負荷テスト(8 Ractor が Time.now を共有ポートに送りつつ GC churn): **0/70**
  (修正前 ~35-50% クラッシュ)。
- 警告なしビルド。

### 4.3 発見と網羅監査
- 並行 GC race を 6 個クラッシュ駆動で発見・修正後、**網羅監査**(全 GC フェーズ × 全共有構造)で
  さらに 2 個(#7 `freed_ractor_local_keys`, #8 `global_hooks`)を発見・修正。
- 監査でクリアした**偽陽性**(追わなくてよい): 共有シェイプツリー(`shape_tree_mark` は
  グローバル GC 専用; per-shape フィールドは write-once-readonly)、dsymbol/fstring
  (shareable→pin で局所 GC は解放しない; かつ concurrent-set)、box_classext(FL_SHAREABLE→main)。
- **generic_fields_tbl / id2ref の per-objspace 化は却下**: frozen shareable オブジェクトの
  generic ivar は生成元 Ractor の表に入り**跨ぎ読み**される(検証済み)ので per-objspace でも
  跨ぎアクセスが消えない。**本質的に共有なデータ → NON_BARRIER ロックが正しい同期**(対症療法ではない)。

---

## 5. 懸念点 / TODO

### 5.0 【既知の潜在バグ・最重要】送信コピーの cross-objspace 所有権
**症状**: Ractor 間で送ったコピー(`basket_type_copy`/`move`)は送信側 S の objspace に物理的に
確保される(`ractor_copy` は送信側コンテキストで `#clone`)。受信(`ractor_basket_accept`)は
`reset_belonging` で所有を受信側 R にするだけで**再配置も再コピーもしない**(no-move)。結果、
**「物理的には S の objspace、論理的な所有は R」**というオブジェクトが生じる。
- in-flight 中(受信前)は fix #4(`rb_gc_pin_in_flight_message` の shared_bits)で S にピンされ、
  さらにグローバル GC が「shareable な Ractor オブジェクト → recv_queue → basket → コピー」の
  エッジ(`gc_shared_relation` で parent=shareable Ractor)で**再ピン**するので生存する。
- **受信後が穴**: basket が free され Ractor→コピーのピン経路が消える。次のグローバル GC は
  コピーを R のルート経由でマークするが、parent が shareable でないので shared_bit を**再付与せず
  クリア**する。その後の **S のローカル GC** はコピーを(S のルートに無く shared_bit も無いので)
  **解放** → R が参照していると **dangling / use-after-free**。
- **テストが通る理由**: 現状テストは受信オブジェクトが短命(受け取って即捨てる)で、
  「受信 → R が長期保持 → グローバル GC → S のローカル GC」の窓に当たらない。**テスト網羅に穴**。
- **根本原因**: 封じ込めモデルの不変条件「オブジェクトは所有者の objspace に住む」を、送信コピーが
  破っている(R 所有だが S に物理在住)。S のローカル GC は R のルートを知らないので解放してしまう。
- **解決策は §6 で検討**。

### 5.1 未監査で原理的に残るカテゴリ(優先順)
1. **ユーザ定義 T_DATA の `dmark`/`dfree` コールバック** — confined ローカル GC の mark/sweep 中に
   任意の C 拡張コードが走り、任意の共有 C 状態を触りうる。**封じ込めモデルの根本的な穴**
   (拡張側の問題で有界化不能)。要設計: custom-dmark を持つ T_DATA はローカル GC でマークせず
   グローバル GC に委ねる等の検討。
2. **JIT (YJIT/ZJIT, Rust)** — `rb_*_root_mark` はグローバル GC 専用経路だが、Rust 側の per-Ractor
   相互作用・JIT コードが GC 外で共有表を触るかは未確認。
3. **`RUBY_INTERNAL_EVENT_FREEOBJ` フック** — confined sweep 中に発火するユーザ/内部コールバックの
   共有状態アクセス未追跡。

### 5.2 性能の follow-up
- グローバル GC の STW バリア(scale.rb N=8 で ~13%)。major を「メモリ圧 or N 回ごと」だけ
  グローバルにするスロットルは試作したが同じ generic-ivar race を誘発して撤回。**race 修正済みの
  現在なら再投入可**(両極端=毎回グローバル/全くグローバルしない、はどちらも test-all green)。
- ロックフリー化の唯一の代替(NON_BARRIER ロックを消す道): `generic_fields_tbl` を**並行ハッシュマップ**化
  (fstring/symbol の `rb_concurrent_set` 同様。ただし concurrent_set は集合専用なので obj→fields の
  並行マップインフラ新設が要る)。大きな別作業。

### 5.3 機能の follow-up(以前から)
- **cc/cme/cc_tbl/shape-edge の main objspace ルーティング**(scaffolding あり=`main_newobj_cache`):
  現状これらはグローバル GC でも pin され続ける(VM 内部 shareable の小さな有界リーク)。main へ
  ルートすればグローバル GC が回収できる。
- **Ractor 終了時の objspace ハンドオフ/解放**: 現状 objspace は終了時にリークする。設計メモ
  (memory)では「終了 Ractor のローカルヒープは最初に join した Ractor が継承(no-move)」。
- **`GC.stat`/`GC.total_time`** が per-Ractor objspace 下でwall-clock のみ(per-objspace 集計が未実装)。
- make_shareable した**ユーザ shareable** はローカル objspace に pin-while-live、グローバル GC でのみ回収。

### 5.4 既知の制約
- 実験実装、**未コミット**。`RGENGC_CHECK_MODE`/`check_rvalue_consistency` は RLGC 非対応
  (cross-objspace 参照を偽陽性で "not a Ruby object" 報告)→ RLGC デバッグには信用しない
  (`rlgc_obj_in_any_heap` で緩和済みだが完全ではない)。
- 保守的マーク中の "out-of-heap" 表示は「**現在の(driver)objspace に無い**」の意味(別 objspace に
  居る有効オブジェクトでも出る)であって「解放済み」ではない。

---

## 6. 解決策の検討（§5.0 の cross-objspace 所有権バグ）

検証 workflow(wf_c60d959a)による評価。

### 6.1 根本原因の精緻化（検証済み）
- **訂正**: `shared_bits` は confined ローカル GC のルートとして実際に使われる(`gc_mark_shared_roots`,
  default.c:5241/6625)。なので fix #4 の in-flight ピンは **queue 中のコピーを正しく守っている**。
- バグは厳密に **`ractor_basket_free` 後の窓**: コピーの生存エッジが「受信側 R の **unshareable な
  root**(ローカル変数/ivar)」だけになる。グローバル GC は `shared_bits` を全クリア(default.c:6744)し
  R の root 経由で再マークするが、parent が shareable でないので `gc_shared_relation`(4951-4964)は
  shared_bit を**再付与しない** → S の次のローカル GC が掃く(default.c:5062 で R→copy エッジは skip
  され、S の root にも無い) → UAF。

### 6.2 候補比較（正しさ > 単純さ > 性能）
| 案 | 正しさ | コスト | 判定 |
|---|---|---|---|
| **A: materialize-on-receive** | ◎ 所有者の objspace に入る | deep-copy + #clone がもう1回 | **推奨(最も単純な完全修正)** |
| F1: 明示 export darray + global 照合 | ◎ | 2回目コピー不要だが永続ルート集合・reconcile・多段転送・終了リークの追加機構 | 正しいが機構が重い |
| B: コピーを main objspace に確保 | ○(UAF は直る) | in-flight+受信済みコピーが main に浮遊し global GC まで回収されず main 肥大・STW 頻発 | コスト不可、却下 |
| F2: shared_bits を cross-objspace 一般化 | ✗ 不完全 | — | **誤り(§6.4)** |

### 6.3 推奨: A（materialize-on-receive、ガード付き）
**変更1箇所**: `ractor_basket_accept`(ractor_sync.c:841-854)。`v = ractor_basket_value(b)` の直後、
例外処理の前に:
```c
if ((b->type == basket_type_copy || b->type == basket_type_move) &&
    rb_gc_get_objspace() != rb_gc_main_objspace() /* 受信側がローカル objspace を持つ */) {
    v = ractor_copy(v);   /* 受信側(=現在)の objspace へ再 clone */
}
```
- `ractor_copy` は受信側スレッドで走るので `rb_gc_get_objspace()`=受信側 → 複製は**受信側 objspace** に
  確保される。`copy_enter` は shareable を再利用し unshareable subtree だけ再 clone、循環は traverse の
  dedup テーブルで停止。move も `move_leave` で source は moved marker 済みなので再コピーは通常コピー扱い。
- **正しさ**: accept 後オブジェクトは受信側の objspace に住み受信側 root が保持 → 受信側ローカル GC が
  通常どおり回収。旧来の cross-objspace エッジは消える。送信側クローンは(次の global GC まで shared_bits
  でピン→その後 unref)通常ゴミになり S が回収 → **dangling 無し・リーク無し・浮遊は従来の shared_bits 寿命まで**。
- in-flight 窓(accept 前)のために `rb_gc_pin_in_flight_message` は **残す**。
- **コスト**: copy/move メッセージ1件につき deep-copy + `#clone` がもう1回。受信はレイテンシ経路で
  スループット経路ではないので許容範囲。一時的にピーク2クローン分のメモリ。
- **主リスク(A を採るなら詰める点)**: `#clone` が受信側でも走るので、ユーザ `clone`/`initialize_clone` の
  副作用が**2回**発火する(観測可能な互換変化)。緩和案: (i) 文書化、(ii) 受信側の再 materialize を
  **`#clone` ではなく副作用の無い内部ディープ複製**にする(traverse は再利用、ノード複製を `rb_obj_clone`
  相当の低レベル複製へ)。ただしカスタム clone を持つオブジェクトを正しく複製できるかは要検証。
- 例外: 再コピー自体が raise しうるので `b->p.exception` 処理の**前**に再コピーし、raise 経路でも
  basket を free してリークさせない。

### 6.4 誤り / 却下
- **F2(`gc_shared_relation` を cross-objspace エッジに一般化)単独は致命的に不完全**: **root エッジが
  不可視**。`gc_mark_set_parent_raw` は root で `parent_object=Qundef`(default.c:5225/5243)、
  `gc_shared_relation` は `SPECIAL_CONST_P(parent)` で早期 return(4954)。`basket_free` 後の生存エッジは
  正に受信側 root なので object→object 検出では救えない。「parent_objspace を持たせ root を自 objspace
  扱い」案でも、ビットが立つのは R の objspace で、コピーが物理的に居る **S ではない**(S の
  `gc_mark_shared_roots` が拾えない)→ S の掃きから救えない。
- **B** は誤りではないが main 肥大 + STW 頻発で却下。

### 6.5 再現テスト（バグを踏む。修正後のみ pass）
`bootstraptest/test_ractor.rb` に追加(`RUBY_RACTOR_LOCAL_GC=1` で実行、SEGV / "mark T_NONE" が出ないこと):
```ruby
assert_equal 'ok', %q{
  port = Ractor::Port.new
  r = Ractor.new(port) do |port|
    10.times do
      obj = { a: "x" * 4000, b: (1..200).map { |i| "s#{i}" }, c: [Object.new, Object.new] }
      port << obj
      1_000.times { "y" * 200 }   # 送信側ローカル objspace を churn
      GC.start
    end
  end
  recv = []
  10.times do
    msg = port.receive            # accept(): basket free。バグ時コピーは送信側 objspace に残る
    recv << msg                   # 受信側が長期保持(root エッジ, parent unshareable)
    1_000.times { Object.new }
    GC.start                      # global GC: コピーの shared_bit がクリアされ再付与されない
  end
  r.take rescue nil
  GC.start; GC.start              # 送信側ローカル GC が unpin されたコピーを掃く
  ok = recv.all? { |m| m[:a].length == 4000 && m[:b].length == 200 && m[:c].length == 2 }
  ok ? 'ok' : 'corrupt'           # 解放済みメモリを読むと corrupt/SEGV
}
```
バグ条件: (a) 受信側が accept 後もコピーを保持、(b) accept 後の global GC が shared_bit をクリア、
(c) 送信側 GC churn が unpin されたコピーを実際に sweep。A 適用後はコピーが受信側 objspace に住み全 churn を
生き残る。

### 6.6 推奨まとめ
**A(materialize-on-receive, guarded)を推奨**。単一箇所・完全修正・リーク無し。唯一の論点は
`#clone` 二重発火で、これが許容できないなら緩和案(ii)(副作用の無い内部複製)で詰めるか、F1(明示 export
集合+global 照合)に切り替える。B は却下。F2 単独は誤り。

### 6.7 理想案「clone 先 = copy 先（受信側 objspace に単一 clone を直接確保）」の評価 — 不採用
**結論: 安全には実現不可能**。これは以下の **トリレンマ（3つのうち2つしか取れない）** に帰着する:
- **(I) スナップショット意味論**: コピーは send 時点で確定し、send 後に送信側が元オブジェクトを
  書き換えてもメッセージに影響しない(現状の意味論; `ractor_copy` が send 時=ractor_sync.c:799 に走る)。
- **(II) lock-free ローカル GC**: 受信側 objspace は `local==TRUE` で cache-miss が無ロック
  (default.c:2859 は local objspace で `RB_GC_CR_LOCK` をスキップ)。~2.6→~4.7 有効コアの源泉。
- **(III) 単一トラバース**: 1回の `#clone` でそのまま受信側 objspace に着地。

3つ同時は不可能:
- **send 時にコピー作成(I)** ⇒ 送信スレッドで走る ⇒ そのまま受信側 objspace に確保するには
  **foreign allocation**（送信スレッドが受信側の無ロックヒープへ書込）= **(II) を壊す**。受信側の
  `heap->free_pages`/arena freelist/`heap_pages.sorted`(中央挿入+realloc, default.c:2385) を、受信側
  自身の無ロック mark/sweep と並行に第2の同期されないスレッドが書き換える。正す唯一の方法は refill 経路へ
  **両側ロック** ⇒ 削除したはずの「アロケーション毎クロス Ractor 直列化」が復活。さらに確保先は**暗黙**
  (`rb_newobj`→`rb_gc_get_objspace()`=現在 Ractor の objspace, gc.c:248-253/1083-1088)で、`#clone` は
  任意ユーザコードの全 newobj が暗黙先へ行く ⇒「clone の出力だけ」を受信側へ向けるのは不可能、
  `#clone` 全動的範囲で送信スレッドの objspace/cache 差し替えが必要 ⇒ 送信スレッドのスタック誤ルート＋
  圧時に送信スレッドが受信側 objspace を GC。
- **receive 時に単一 clone 作成(III+受信側着地)** ⇒ **(I) を壊す**(defer-clone: send 後の変更が漏れる)
  + 受信キューが送信側の生きた unshareable を参照(§5.0 より悪い live cross-objspace edge)。

→ **(I)+(II) を守る道は「2回目のトラバースを受信側で行う」= A / A2 のみ**(単一トラバースを諦める)。

**foreign-alloc 前例は死んでいる(重要)**: `main_newobj_cache`(ractor_core.h:117) と
`rb_gc_ractor_cache_alloc_on_main`(gc.c:3914) は**呼び出し元ゼロの休眠スキャフォールド**(grep 確認済)。
実際に non-main→main 確保が安全なのは main が `local==FALSE` で refill が CR ロック(default.c:2859)＋
global STW でしか回収されないから ＝ 受信側ローカルヒープの**真逆**。将来「clone-into-receiver」を誤った
前提で作らないよう、この死んだスキャフォールドは削除かコメントで無効と明記すべき(別タスク)。

**単一 #clone を保つ最近接 = A2(relocate-on-receive)**: 受信スレッドで move 型 relocate
(`rb_obj_traverse_replace(v, move_enter, move_leave, true)`, ractor.c:2089 を模す)を accept 地点
(ractor_sync.c:842)で走らせる。確保は無ロックのまま(受信スレッドが自ヒープへ確保, ractor.c:2050)、
`#clone` は send の1回だけ。ただし:
- 新たな **cross-objspace SOURCE アクセス**ハザード: R が S の並行回収中ヒープの元グラフを読み、move なら
  `move_leave` の memzero で zombify(ractor.c:2083)。in-flight ピン(gc.c:3530)は **free** は防ぐが
  並行な header/bitmap アクセスは防がない。obj_id/generic_ivar の付替え(ractor.c:2073-2078)は VM-global
  表に触れ NON_BARRIER ロックが要る。
- **movability ギャップ回帰**: copy は受理する T_DATA(ractor.c:1972) を move は拒否(1976) ⇒ 当該ノードだけ
  再 `ractor_obj_clone` fallback ⇒ そこだけ二重副作用が復活。
- これらを塞ぐ握手プロトコルが要り、現状ツリーに前例なし。**A の「封じ込められた二重副作用」より
  correctness/complexity リスクが高い**。なお default.c の `gc_move` は intra-objspace 専用
  (`gc_update_references` 全ヒープパス前提)で流用不可。

**最終判断（優先順位による強制）**: 正しさ・スナップショット・lock-free(優先 1-3) > 単一 clone(優先 4)
なので **Solution A を推奨**。単一 clone(II 維持)が*どうしても*必須なら、A2 を上記握手 + T_DATA fallback
込みで実装する(より高リスク)。理想 (I)+(II)+(III) は構造的に到達不能。

### 6.8 「ウルトラC」案: 送信スレッド T1 を一瞬 R2 に所属させる — 不採用（検証済）
**着眼**: allocation 先が暗黙(現在 Ractor の objspace)なので、T1 の「現在 Ractor」を clone の間だけ R2 に
すれば単一 clone で受信側着地。clone は send 時に T1 で走るのでスナップショット(I)も保てる ── 理想
(I)+(III) を取りに行ける唯一の角度。2変種: **V1** 全身分フリップ(`th->ractor` を R2 に) / **V2**
allocation 先だけ TLS で R2 に上書き(身分は R1 のまま)。

**結論: どちらも一般機構としては不可。Solution A を採る。** 検証済みキラー:
1. **objspace と newobj_cache は密結合ペア**(キャッシュの freelist は当該 objspace の page から切り出す,
   default.c:2876)。`rb_newobj` は cr を二度・不整合に読む(gc.c:1087 が `cr->newobj_cache`、gc.c:1088 が
   `rb_gc_get_objspace()`=TLS)。objspace だけ R2 に向けると R1 のキャッシュ slot を R2 帳簿で配る →
   オブジェクトは R1 に着地(**元バグ再現**)＋次 miss で cross-heap 破壊。よって V2 は R2 の実キャッシュも
   借りる必要 → R2 の無ロック freelist への **foreign 並行 writer**。
2. **「Ractor 1個だけ止める」プリミティブが存在しない**。唯一の停止は global barrier(thread_pthread.c:1455)
   で VM ロック下に全 Ractor を STW ＝ **RLGC が削除したまさにその全体直列化**。R2 を止めるには削除した
   ものを復活させることになる。`ractor_wakeup` は停車中を起こせるだけで、走行中を強制停車はできない。
3. **ローカル GC のルート集合は駆動スレッドの EC に固定**(gc.c:199 が `GET_EC()`=T1 を捕捉; gc.c:3387/3396)。
   T1 駆動で R2 ヒープを GC すると R1 のルート＋T1 スタックをマークし **R2 自身の生存ルート(受信済み
   メッセージ・local storage・停止中スレッドのスタック)をマークしない** → R2 の到達可能オブジェクトを
   sweep → UAF。単一 EC の vm_context では「身分=R2 だが T1 スタックを走査」が表現不能。よって clone 中に
   R2 ヒープが埋まると回収不能 → GC 無効化+ヒープ growth、枯渇したら A に bail。
4. **V1 はさらに致命**: ユーザ `#clone` が R2 身分で走る → `Ractor.current`==R2、`Ractor[]` が R2 の
   locals を破壊、`Ractor.receive` が R2 のキューから**メッセージ窃取**(VM_ASSERT cr==rp->r が通る)。
   加えて per-ractor GVL スケジューラ run-queue、barrier/deadlock スレッド数、GC ルート所有を破壊
   (`th->ractor` は他ネイティブスレッドからロック無しで並行読み)。
5. **非同期 send が同期ランデブー化**: `ractor_send_basket`(ractor_sync.c:1178-1194)は enqueue→wakeup→即
   return で送信側は待たない。R2 停止を挟むと相互 send R1↔R2 でデッドロック、fan-in で直列化、safepoint の
   無い計算中 R2 で送信側が無制限ブロック。

**境界の論拠（設計意図そのもの）**: 本実装は **recv_queue/ports だけ** を per-ractor mutex で
foreign-writer-safe にし(`rb_gc_during_confined_local_gc_p`)、**allocation ヒープ/freelist/newobj_cache は
意図的に完全 lock-free・foreign-writer 非対応のまま**残した。borrow-R2 はちょうどこの境界を侵犯する。

**生き残る最小形**: 「R2 が receive で停車中と確証できる時だけの opportunistic fast-path、それ以外は
Solution A に fallback」。要件: R2 停車ラッチ + TLS alloc-target 上書き(objspace+cache を原子的に) +
CHECK_MODE の belonging-id 上書き + clone 中 GC 無効化+ヒープ growth + `th->ractor`==R1 維持。これでも
回収できないので枯渇時 A に bail ＝「**A のコード経路を丸ごと内包する、スケジューリング状態依存の脆い
最適化**」。割に合わない。

**A vs borrow-R2**: A は受信スレッドが本当に current なので rb_gc_get_objspace/newobj_cache/lock-free 判定/
belonging/vm_context.ec/スタック走査/GC ルートが**構造上すべて自動的に正しい**・新規機構ゼロ。borrow-R2 は
存在しない単一 Ractor 停止・foreign ヒープへの GC 無効化+growth・3者協調上書き・分岐 GC ルート経路という
前例皆無の並行機構を投じ、保護対象の lock-free 性を攻撃して、**たった1回の #clone を(しかも R2 が暇な時
だけ)節約**するだけ。割に合わない。

**将来 double-clone コストが実測で問題化したら**、最初に試すのは borrow-R2 ではなく: (a) ユーザ
`#clone`/`initialize_copy` を持たないグラフは2回目を C レベル構造コピー(Ruby コールバック無し)に
(§6.3 緩和案(ii) と同じ)、または (b) **move** send 限定の page/所有権 relocate 最適化。

### 6.9 実装と検証（Solution A 採用・実装済み・未コミット）
**変更（3 ファイル + テスト）:**
- `ractor_sync.c` `ractor_basket_accept`: basket を先に free（再コピー/raise でのリーク防止）後、
  copy/move かつ `!rb_gc_object_in_current_objspace_p(v)` のとき `v = ractor_copy(v)` で受信スレッド
  =受信側 objspace へ再 materialize。例外も再 materialize 後に raise（例外オブジェクトも受信側へ）。
  受信側からは in-flight ピンを **再設定しない**（送信側 objspace の shared_bits への cross-objspace
  write race になるため。ピンは送信時に設定済みで accept 時点では有効）。
- `gc.c` + `internal/gc.h`: `rb_gc_object_in_current_objspace_p(VALUE)` 追加 ── 現在 objspace の
  ページ集合のみ参照（`rb_gc_impl_pointer_to_heap_p`）、呼び出しスレッドが所有するので **VM バリア不要**
  (`rb_gc_conservative_owner` は全 Ractor 走査で要バリア＝accept では使えない)。非RLGC では単一 objspace
  ゆえ常に true で自動 no-op、self-send も no-op。
- `bootstraptest/test_ractor.rb`: 決定的回帰テスト追加。

**検証（重要な経験則）:** 素朴なストレスでは**踏めない**。`GC.start` は full=major=**global GC** で
main の root から copy を必ず mark し生かすため。バグを踏むには orchestrated 手順が要る:
(1) global GC でピンを消し copy を young(age1, RVALUE_OLD_AGE=3)のまま残す → (2) sender が
`GC.start(full_mark: false)`=**confined minor**(default.c:7190 で global_gc=false)で young 未ピン copy を
掃く → (3) freed slot を同形状 junk で上書き。結果:
- **修正無し（再コピー bypass）: 6/6 SIGABRT** = `[BUG] try to mark T_NONE object (... parent:
  out-of-heap ...)`(default.c:5047)── main の参照が sender objspace の解放済みオブジェクトを指す＝
  §5.0 の UAF が顕在化。
- **修正有り: 6/6 ok**。
- `test_ractor.rb` フル(161件)RLGC ON/OFF とも **159/161**。残り 2(#118/#121 Tempfile/fileno,
  行1917/1974)は **RLGC OFF でも同一失敗の既存環境要因**(miniruby/Tempfile, -O0 スタック)で本変更と無関係。
- 新テスト単体: フル ruby RLGC ON 5/5・OFF 2/2、gc.c/ractor.c 警告なし。

### 6.10 ストレステスト結果（多角的、2026-05-31）
コア修正に対し多数のシナリオを RLGC ON で反復実行:
- **クリーン(crash 0)**: 決定的 clobber(1送信) 25/25、move 10/10、グラフ(循環/別名/shareable leaf) 10/10、
  shareable-ref 10/10、value 往復(200 Ractor) 10/10、GC.stress 10/10。
- **エキゾチック型 33/36 がクラッシュ無し・値正**（大 Bignum、Rational/Complex、Float 特殊値[±Inf/NaN/±0]、
  ASCII-8BIT/UTF-16/壊れUTF-8 エンコーディング・coderange、埋め込み/heap 文字列、動的/Unicode シンボル、
  Struct/Data、多 ivar(shape)、1000段ネスト、compare_by_identity 等)。残り 3 は `initialize_clone`/
  `initialize_copy` の**呼び出し回数チェックが count==2 で fail** ＝ §6.3 の「`#clone` 2回発火」を
  ストレスが**実証**したもので破損ではない（既知トレードオフの確証）。

**残存バグ①（再コピー × 並行 global GC、~7.5%）:** 4並行送信 + 毎メッセージ global GC + 大量 clobber +
受信側 long-term hold という**極限**シナリオ(s1)でのみ ~3/40 crash。署名は `[BUG] try to mark T_NONE`、
parent = メッセージの :b 配列が **`len:150 capa:1` の破損ヘッダ**で out-of-heap(sender objspace)の解放済み
要素を参照 ── 受信側 `ractor_copy` が並行 global GC 下で**不完全/破損したクローンを生成**している。切り分け:
fix OFF=14/15(§5.0 主バグ)→ fix ON=~7.5% で主バグは解消、残りは別系統。`held` しない版は 0、純 in-flight/
clobber も 0 ＝「**受信側 re-copy + 長期保持**」が必要。**holder 案(§旧6.9)は実装して棄却**: 60回比較で
holder有 4/60 vs holder無 5/60 と**有意差なし(無効)** → コードから撤去。root cause は ractor_copy と RLGC
global GC の並行性堅牢性(スロット破損)で、s2(下記)と同系統の疑い。**未解決・要 RLGC 深層対応**。

**残存バグ②（既存 RLGC バグ、本修正と無関係）:** non-main↔non-main 交換 + main サブスレッドの並行
`GC.start` bomb(s2)で **6/6 SEGV**、`rb_execution_context_mark`(vm.c:3730, fiber/EC マーク)中。
**RLGC OFF では 0**、fix ON/OFF で同一 ＝ メッセージ所有権修正とは独立した、RLGC の「並行 GC vs 走行中
スレッド/fiber の EC マーク」レース。別タスクとして要対応。

**結論:** コア修正(materialize-on-receive, holder 無し)は §5.0 を解消し、現実的・型多様なワークロードで
クラッシュ無し。極限並行ストレスで露呈した残存①②は RLGC 並行 GC の深層堅牢性課題で、本修正の単純な追補
では閉じない（holder は無効と実証）。
