# Ractor-local GC 設計ドキュメント

CRuby の GC を **Ractor ごとに独立した objspace** へ分割し、各 Ractor が自分のヒープを
**Stop-The-World なしで並行に** mark/sweep する実験実装。`gc/default/default.c` のみ対象
(mmtk/wbcheck は対象外)。本ドキュメントだけで第三者が同等実装を再現できることを狙う。

**ステータス**: 実験実装。ブランチ `ractor-local-gc` に3コミット(`master` は無改変)。

- 既定 OFF。`RUBY_RACTOR_LOCAL_GC=1` で有効化。
- 性能: bench.rb N=8 で **~4.7 実効コア**(直列ベースライン比 4.75倍)、最大8個のローカル GC が同時実行。
- 正しさ: `make test-all` env-on **34849 / 0 failures**、env-off 0 failures。`make btest`
  `test_ractor.rb` **159/161**(残り2は miniruby/Tempfile の既存環境要因で本実装と無関係)。
- 既知の未解決課題: **cc/cme・メッセージコピーの cross-objspace 寿命**(§5.1)。極限並行ストレスでのみ
  顕在化する UAF で、RLGC の設計レベルの対応(main-routing)が必要。通常〜型多様ワークロードはクラッシュ無し。

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
   soundness: s→u エッジは常に **u の所有者**が作る(隔離則: 他 Ractor の unshareable 参照は
   持てない)ので、自分のオブジェクト u にビットを立てれば良い(親 s は触らない)。
5. **VM 内部キャッシュのピン**: method/inline cache 等は shareable で cross-Ractor に共有され
   弱インラインキャッシュ等で辿れるので、ローカル GC が回収しないよう「born-shareable は
   shared_bits」+ 「sweep で cc/cme/callinfo の imemo は常にピン」。**ただしこのピンには既知の
   寿命バグがある(§5.1)。**
6. **共有可変構造へのアクセスは同期**: 並行ローカル GC が触らざるを得ない VM グローバル/Ractor 固有の
   可変構造は **NON_BARRIER ロック**(VM ロックだがグローバル GC バリアに途中参加しない版)等で同期
   (§3.7)。
7. **EC/fiber の封じ込め**: ローカル GC は**自 Ractor の実行コンテキスト(EC)のみ**を歩く。他 Ractor の
   EC は並行実行中でフレームスタックが不安定なので走査してはならない(§3.11)。

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
- `RACTOR_LOCAL_GC_AUDIT`(コンパイル時 1) — confinement 監査(s→u WB 完全性 + u→s sweep 不変条件、§3.12)。

---

## 2. 変更点一覧 (master との diff, コード分)

```
 gc.c                 | 298 +    VM 側グルー: objspace ルーティング, conservative_owner,
                       |          全 objspace 走査, 各種同期, ローカルルート, 各種ヘルパー
 gc/default/default.c | 863 +    GC 本体: per-objspace objspace, confined mark, shared_bits,
                       |          global GC, lock-free alloc + arena, 並行 race 修正
 ractor.c             |  38 +    per-Ractor objspace 生成, ローカルルートマーク, cache_free(r)
 ractor_core.h        |   9 +    rb_ractor_t に local_gc_objspace / main_newobj_cache
 ractor_sync.c        |  56 +    メッセージポートのマークを per-Ractor ロック, in-flight pin,
                       |          materialize-on-receive (§3.10)
 cont.c               |  12 +    confined GC で他 Ractor の fiber/EC を歩かない (§3.11)
 vm.c                 |  32 +    gen_fields_cache を強ルート化, thread roots, EC-confinement assert
 variable.c           |   6 +    generic_fields_tbl sweep-delete を NON_BARRIER
 id_table.c           |   5 +    managed_id_table_dup に RB_OBJ_SET_SHAREABLE (グローバル GC 根本修正)
 iseq.c               |   6 +    TracePoint/cc クリアを全 objspace に
 internal/gc.h        |  22 +    新 API の宣言
 bootstraptest/...    |  44 +    materialize-on-receive の決定的回帰テスト
```

機能対応:
- **per-Ractor objspace ライフサイクル**: ractor.c, gc.c, ractor_core.h, default.c — §3.1
- **confined mark / 封じ込め**: default.c, gc.c — §3.2
- **shared_bits + WB**: default.c — §3.3
- **global GC**: default.c, gc.c — §3.4
- **lock-free alloc + arena**: default.c — §3.5, §3.6
- **並行 GC race 修正(8件)**: §3.7
- **根本バグ修正**: id_table.c, vm.c, default.c — §3.8
- **全 objspace 走査**: gc.c, iseq.c — §3.9
- **メッセージ所有権(materialize-on-receive)**: ractor_sync.c, gc.c, test — §3.10
- **EC/fiber 封じ込め**: cont.c, gc.c, vm.c — §3.11
- **confinement アサーション**: vm.c, default.c, gc.c — §3.12

---

## 3. 変更点詳細

### 3.1 per-Ractor objspace ライフサイクル

**`rb_ractor_t` に2フィールド追加** (ractor_core.h):
```c
void *local_gc_objspace;   /* この Ractor の objspace。main は VM の objspace を別名参照 */
void *main_newobj_cache;   /* main objspace 割り当て用の足場 (現状 未配線・休眠。§A.2/§5.4) */
```

**生成** (ractor.c `vm_insert_ractor0`): 非 main Ractor 作成時、`rb_gc_rlgc_enabled() && r != main`
なら `r->local_gc_objspace = rb_gc_objspace_alloc_local()`。main は `rb_ractor_main_alloc` で
`r->local_gc_objspace = GET_VM()->gc.objspace`。

**ルーティング** (gc.c `rb_gc_get_objspace`): 現在 Ractor の `local_gc_objspace`(無ければ VM objspace)。

**newobj cache は所有 objspace に紐づく** (gc.c `rb_gc_ractor_cache_alloc/free`):
cache はその Ractor の objspace からページを引く。`rb_gc_ractor_cache_free(rb_ractor_t *r)`
は **r->local_gc_objspace に対して**解放。シグネチャを `(void *cache)` から `(rb_ractor_t *r)` に変更。

**objspace 初期化の特別処理** (default.c `rb_gc_impl_objspace_init`): 最初の per-Ractor objspace が
出来た瞬間から `rlgc_has_local=true` とし、`objspace->local=TRUE` / `dont_incremental=TRUE`、main も
`gc_rest`+`dont_incremental`。理由: その瞬間から「どの objspace の major もグローバル STW GC」になり、
グローバル GC のバリアが main を**コレクション途中で**捕まえると統一 mark/sweep が破綻するため、main も
incremental/lazy を切ってアトミックにする。

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
  main/グローバル GC が生かす。`global_hooks` もここでは触らない(§3.7 #8)。

**ローカルルートの肝** (ractor.c `rb_gc_mark_ractor_local_roots`, vm.c `rb_gc_mark_thread_roots`):
VM スタックを持つ ec を直接マークする(`thread_mark`→`rb_execution_context_mark(th->ec)`)。
これが「object-heavy なローカル Ractor が全部クラッシュ」していた根本原因の修正:
VM スタック上の live local が fiber wrapper(main objspace, foreign)経由でしか辿れず、confined mark が
skip して use-after-free していた。

### 3.3 shared_bits remset + write barrier

**データ構造** (default.c): `heap_page::shared_bits[]` + `flags.has_shared_objects`、`GET_HEAP_SHARED_BITS`。

**WB** (`rb_gc_impl_writebarrier(a, b)`): 通常の世代別 WB の**前**に、`b` が shareable なら `b` に、
あるいは `a` が shareable(or shared)で `b` が unshareable なら境界の子 `b` に shared_bit を立てる。

**born-shareable** (`newobj_init`): `FL_SHAREABLE` なら生成時に shared_bits をセット。

**ルートパス** (`mark_roots` → `gc_mark_shared_roots`): `has_shared_objects` なページの shared_bits を
走査し各境界オブジェクトを `gc_mark`(subtree も辿る)。

**full mark 中の再計算** (`gc_mark`→`gc_shared_relation`): 親 `rgengc.parent_object` が shareable で
子が unshareable なら shared_bits を立て直す(AUDIT モードは WB 漏れを `gc_shared_wb_miss` で報告、§3.12)。

**クリア規則** (`rgengc_mark_and_rememberset_clear`): `!rlgc_has_local || rlgc_global_gc_active`
のときのみ shared_bits クリア。**ローカル GC はクリアしない**。**移動時** (`gc_move`): shared_bit を src→dest。

### 3.4 global GC

**判定** (`gc_start`, gc_enter の前): `rlgc_has_local && rlgc_global_gc_enabled()` かつ full mark に
なるなら `objspace->flags.global_gc = TRUE`。minor は confined ローカルのまま。

**gc_enter / gc_exit**: ローカル minor は `lock_lev=0` + `flags.local_gc=TRUE`(VM ロックもバリアも取らない)。
グローバル/main は `RB_GC_VM_LOCK()` + `rb_gc_vm_barrier()`(STW)。

**駆動** (`gc_start`): `rlgc_global_gc_active = flags.global_gc;` → `gc_marks` は VM 全ルート + 全 objspace
横断トレース(全 Ractor 停止済み)。完了後 `gc_global_sweep` = `rb_gc_foreach_objspace(gc_global_sweep_one)`
で全 objspace を sweep(dead shareable 回収)。clear も `gc_marks_start` で全 objspace 化。

**保守的マーク** (`rb_gc_impl_mark_maybe`): グローバル時は `rb_gc_conservative_owner()` で全 objspace 横断の
安全な所属判定(ワードをデリファレンスしない)。

**sweep の pin ガード** (`gc_sweep_plane`):
```c
if (objspace->local && RB_OBJ_SHAREABLE_P(vp) && !rlgc_global_gc_active) break; /* ローカルは shareable 不解放 */
if (rlgc_has_local && !rlgc_global_gc_active && RB_OBJ_SHAREABLE_P(vp) && T_IMEMO && (callcache|callinfo|ment)) break;
                                                              /* cc/cme/ci ピン。global GC では guard を外す(§5.1): dead クラスと一緒に回収 */
```

### 3.5 lock-free allocation
`newobj_cache_miss`: ローカル objspace かつ lockfree 有効なら **VM ロックを取らない**(`main` objspace のみ
`RB_GC_CR_LOCK`)。ローカル objspace は Ractor 専有 → 空きページ確保もローカル GC も自スレッドのみ(Ractor GVL
で直列)+グローバル GC はバリアで先に止める、ので VM グローバルロック不要。

### 3.6 per-objspace arena allocator
per-page `mmap`+`munmap`(64KiB アライン用)が process-wide `mmap_lock` を直列化する対策。
`RLGC_PAGE_ARENA_BODIES 256`(=16MiB)・`RLGC_ARENA_ALIGN 2MiB`、`mmap` を 2MiB アラインへ切上げ slack は
**munmap せず**放置、`madvise(MADV_HUGEPAGE)`、freelist は本体メモリ自身に next を格納、objspace 解放時に
arena を munmap。VM グローバル span `[lomem, himem)` を `rlgc_span_extend`(atomic CAS)で更新。

### 3.7 並行ローカル GC が共有データを触る競合の修正(計8件)

根本パターン: ローカル GC が並行実行されると VM グローバル/Ractor 固有の可変構造を他 Ractor の mutator/GC と
同時に触る。**無同期** or **barrier-aware ロック**(`RB_VM_LOCKING` は保留中グローバル GC バリアに途中参加 →
objspace を中途半端な状態で渡す)or **VM グローバル GC スクラッチの読み**は競合する。鍵: グローバル GC バリア
発行側(`rb_ractor_sched_barrier_start`)は待機前に VM mutex を**解放**するので、ローカル GC が **NON_BARRIER**
で VM ロックを取ってもデッドロックしない。

| # | 構造 | 競合 | 修正 |
|---|------|------|------|
| 1 | `generic_fields_tbl_` | mark lookup / sweep delete が writer の rehash と競合 | **NON_BARRIER VM ロック** (gc.c `gc_mark_generic_ivar_sync`, variable.c) |
| 2 | `id2ref_tbl` | sweep delete が無同期 | **NON_BARRIER VM ロック** (gc.c `obj_free_object_id`) |
| 3 | Ractor ポート `recv_queue`/`ports`/`monitors` | foreign sender が per-Ractor mutex で変更、ローカル GC が無ロック走査 | **生 `rb_native_mutex_lock(&r->sync.lock)`** を `rb_gc_during_confined_local_gc_p()` のとき取得 (ractor_sync.c) |
| 4 | in-flight メッセージコピー | 送信側 objspace に居るが受信側 basket からのみ参照 → 両 confined GC が skip → 解放 | 送信側で **shared_bits pin** (gc.c `rb_gc_pin_in_flight_message`) |
| 5 | `vm->gc.mark_func_data` | S の reachability チェックが R の実 GC マークを乗っ取り | 実 GC は `during_gc` で判定し無視 (gc.c `RB_GC_MARK_OR_TRAVERSE`) |
| 6 | per-EC `gen_fields_cache` | weak 参照で sweep に解放され得る | **強 movable ルート化** (vm.c `rb_execution_context_mark`) |
| 7 | `freed_ractor_local_keys` | `rb_ractor_finish_marking` が毎回 free+clear → 二重 free | STW のみ実行 (default.c `gc_marks_finish`) |
| 8 | `vm->global_hooks` | ローカル GC が無ロック走査、writer も無ロック | ローカルルート枝から**除去**(user hook は `r->pub.hooks`=`ractor_mark`、global_hooks は STW グローバル GC) (gc.c) |

### 3.8 根本バグ修正(クラッシュ駆動で発見)
- **id_table.c `rb_managed_id_table_dup`**: `RB_OBJ_SET_SHAREABLE(obj)` 追加。dup された shape-tree
  edge テーブルが unshareable のままだとローカル GC が回収 → グローバル GC の `shape_tree_mark` が T_NONE。
  **これがグローバル GC を default-on にできた根本修正**。
- **default.c `rb_gc_impl_copy_finalizer`**: obj/dest が別 objspace のとき各々の finalizer_table を使う。
- **vm.c thread roots (§3.2)**: VM スタックを直接マーク。

### 3.9 全 objspace 走査
`rb_objspace_each_objects_all_ractors`(gc.c, バリア保持下)を新設し、iseq.c の TracePoint 有効化 / cc クリアが
全 Ractor の iseq を対象にするよう変更(従来は現 Ractor の objspace のみ)。

### 3.10 メッセージの所有権: materialize-on-receive

**背景の問題**: Ractor 間で送ったコピー(`basket_type_copy`/`move`)は、送信側 S のコンテキストで `ractor_copy`
(=`#clone`)が走るため **S の objspace に物理的に確保**される。受信(`ractor_basket_accept`)は
`reset_belonging` で所有を受信側 R にするだけで再配置しない。結果「物理的には S・論理的所有は R」という
不変条件(§1.2-2「オブジェクトは所有者の objspace に住む」)違反のオブジェクトが生じる。in-flight 中は fix #4
+グローバル GC 再ピン(Ractor→recv_queue→basket→copy, parent=shareable Ractor)で生存するが、`basket_free`
後にこの経路が消えると、グローバル GC が shared_bit をクリアし(R の root は unshareable で `gc_shared_relation`
が再付与しない)、S のローカル GC がコピーを解放 → R 参照で **UAF**。

**修正(実装済み)**: 受信時に**受信側 objspace へ再 materialize** する。`ractor_basket_accept`
(ractor_sync.c):
```c
VALUE v = ractor_basket_value(b);
const enum ractor_basket_type type = b->type;
const bool exception = b->p.exception;
const VALUE sender = b->sender;
ractor_basket_free(b);                          /* 先に free(再コピー/raise でのリーク防止) */
if ((type == basket_type_copy || type == basket_type_move) &&
    !rb_gc_object_in_current_objspace_p(v)) {
    v = ractor_copy(v);                          /* 受信スレッド=受信側 objspace へ再 clone */
}
if (exception) rb_exc_raise(ractor_make_remote_exception(v, sender));
return v;
```
- `ractor_copy` は受信スレッドで走るので確保先は受信側 objspace。`rb_gc_object_in_current_objspace_p(VALUE)`
  (gc.c) は現在 objspace のページ集合のみ参照(`rb_gc_impl_pointer_to_heap_p`)で **VM バリア不要**、非RLGC・
  self-send では常に true で自動 no-op。受信側からは shared_bits を**再設定しない**(送信側 objspace への
  cross-objspace write race になるため)。in-flight 窓のため送信側の `rb_gc_pin_in_flight_message` は残す。
- **正しさ**: accept 後オブジェクトは受信側 objspace に住み受信側 root が保持 → 受信側ローカル GC が通常回収。
  送信側クローンは通常ゴミになり S が回収。
- **コスト/互換**: copy/move 1件につき `#clone` がもう1回(send+receive で計2回)。ユーザ
  `clone`/`initialize_clone` の副作用が2回発火する(観測可能な互換変化)。
- **検証**: 決定的再現テスト(`bootstraptest/test_ractor.rb`、orchestrated に global GC でピンを消し
  confined minor で young 未ピン copy を掃いて slot を上書き)。修正無し 6/6 SIGABRT
  (`try to mark T_NONE`)→ 修正有り 6/6 ok。
- **残存**: 極限並行ストレス(4並行送信+毎メッセージ global GC+大量 clobber+長期保持)でのみ ~7.5% で別系統の
  UAF が出る。受信側 `ractor_copy` が並行 global GC 下で破損クローンを生成するもので、§5.1 と同系統。

### 3.11 confined GC と他 Ractor の fiber/EC (バグ修正済)

**症状**: 子を生成した親 Ractor の confined GC 中に SEGV(`rb_execution_context_mark` →
`cont_mark` → `fiber_mark`)。**最小再現(決定的・10/10 crash)**:
```ruby
parent = Ractor.new do
  child = Ractor.new { 200_000.times { [Object.new, "s" * 5] }; :child }
  300.times { GC.start(full_mark: false); 300.times { "x" * 50 } }
  child.value; :done
end
parent.value
```
**根本原因**: `Ractor.new` は親スレッド上で子の root fiber を確保するため、**子の fiber オブジェクトが
物理的に親の objspace に在住**する(§3.10 と同型)。親の confined GC がそれをマーク →
`rb_execution_context_mark` が**子の並行実行中のフレームスタック**を歩く → 壊れた EP で SEGV。二分で確定:
GLOBAL_GC=0/LOCKFREE=0 でも発生(confined GC 固有)、RLGC OFF で消滅。

**修正**: `cont_mark`(cont.c) で、confined local GC 中に**別 Ractor 所有**の cont/fiber は
saved_ec/VM スタック/machine スタックの走査を**スキップ**(その Ractor 自身の GC が自分の EC をマーク;
cont オブジェクトと thread 参照は生かす)。所有判定は `cont->saved_ec.thread_ptr->ractor` を新ヘルパー
`rb_gc_confined_foreign_ractor_p(owner)`(gc.c: confined GC 中かつ owner≠driver で true、global STW 中は
常に false)で行う。検証: 最小再現 **0/30**、s2(non-main↔non-main+GC bomb) GLOBAL_GC=0 で **5/5→0/12**、
btest 159/161。

### 3.12 confinement アサーション (RACTOR_LOCAL_GC_AUDIT / VM_CHECK_MODE)
- **EC-confinement** (vm.c `rb_execution_context_mark` 先頭, `VM_ASSERT`): confined GC は自 Ractor の EC のみ
  歩く(`ec->thread_ptr==NULL || !rb_gc_confined_foreign_ractor_p(ec->thread_ptr->ractor)`)。§3.11 種別を捕捉。
- **u→s liveness** (default.c sweep, AUDIT, rb_bug): confined local GC は shareable を決して free しない
  (sweep pin 迂回の検出)。
- **s→u WB-miss** (default.c `gc_shared_wb_miss`, AUDIT): shareable→unshareable で shared_bit 未記録の
  境界エッジを報告。
- 検証: VM_CHECK_MODE=1 + AUDIT=1 でコンパイル成功、正常系で誤発火 0。これらは §5.1 の診断に有用
  (例: s→u WB ミスが 0 ＝ cc/cme バグは WB 系ではない、と確定できた)。

---

## 4. 成果

### 4.1 ベンチマーク
ワークロード: N Ractor が各自 `300 回 { a=[]; 3000.times{ a << [i, "s#{i}", {k=>i}] }; a.clear }`。
AMD Ryzen 9 5900HX (8 物理/16 HT)。

| 構成 | N=1 | N=8 | 実効コア | CPU% |
|------|-----|-----|------|------|
| env-off (共有 objspace・直列) | 0.199 | 1.568 | **1.0** | 244% |
| RLGC 修正前 (cache-miss ロックあり) | 0.186 | 0.562 | 2.6 | 362% |
| **RLGC フルロックフリー (本実装)** | 0.189 | **0.330** | **~4.7** | 658% |

直列比 **4.75倍**。`RLGC_STATS`: `local GCs: 431 (max 8 ran concurrently), global GCs: 8`。
残る 8→4.7 のギャップは (a) グローバル GC の STW バリア(~13%)、(b) GC 自体の CPU コスト。

### 4.2 正しさ
- `make test-all` env-on: **34849 / 0 failures**、env-off **34837 / 0 failures**(非 RLGC 経路に回帰なし)。
- `make btest` `test_ractor.rb`: **159/161**(残り2は #118/#121 Tempfile/fileno で RLGC OFF でも同一失敗の
  既存環境要因=miniruby+Tempfile, 本実装と無関係)。
- [Bug#18117] ポート負荷(8 Ractor が共有ポートに Time.now 送信+GC churn): **0/70**(修正前 ~35-50% クラッシュ)。
- 警告なしビルド。

### 4.3 ストレステスト
コア(§3.10/§3.11)修正に対し多角的に反復:
- **クラッシュ 0**: 決定的 clobber、move、グラフ(循環/別名/shareable leaf)、shareable-ref、value 往復
  (200 Ractor)、GC.stress、fiber/EC 最小再現。
- **エキゾチック型 33/36 がクラッシュ無し・値正**(大 Bignum、Rational/Complex、Float 特殊値、各種
  エンコーディング/coderange、シンボル、Struct/Data、多 ivar、深いネスト、compare_by_identity 等)。残り 3 は
  `initialize_clone` の呼び出し回数チェック(`#clone` 2回発火、§3.10)を捕捉したもので破損ではない。
- 極限並行ストレスでのみ §5.1 の UAF が顕在化(~7.5%)。

### 4.4 発見と網羅監査
- 並行 GC race を 6 個クラッシュ駆動 + 網羅監査で 2 個(`freed_ractor_local_keys`, `global_hooks`)発見・修正。
- 偽陽性(追わなくてよい): 共有シェイプツリー(`shape_tree_mark` はグローバル GC 専用)、dsymbol/fstring
  (pin+concurrent-set)、box_classext(FL_SHAREABLE→main)。
- **generic_fields_tbl / id2ref の per-objspace 化は却下**: frozen shareable の generic ivar は生成元 Ractor の
  表に入り跨ぎ読みされるので per-objspace でも跨ぎが消えない。本質的に共有 → NON_BARRIER ロックが正しい同期。

---

## 5. 残存課題

### 5.1 mark-T_NONE 並行族 — shareable VM インフラの解放が並行 GC と非整合
2026-05-31 の徹底ストレス(36 シナリオ)で判明した最大の残存系統。「**生き残った構造が、別 objspace で
解放されたオブジェクトを参照して dangling**」が共通根で、多数の症状を生む。

**この系統の中で修正できた個別インスタンス（コミット済み）**:
- **③ confined GC が他 Ractor の fiber/EC を歩く**(§3.11) ── commit a19485dfc。
- **cc/cme dangling**(commit e94190497): **クラスは全て shareable**(`Ractor.shareable?(Class.new)==true`)
  なので、匿名クラス k は confined GC では解放されず **global GC が到達性で回収**する。バグは cc/cme pin が
  **全 GC（global 含む）**で効き、dead クラスが global GC に回収されるのに cme が pin で生き残って owner を
  dangling 参照していたこと。pin を `!rlgc_global_gc_active` でゲート(shareable pin と同じく global GC では
  guard を外す)→ r4 4/20→**0/40**、s2 2/12→**0/15**。live クラスは m_tbl/cc_tbl から cc/cme を強くマーク
  するので生存、dead クラスは subtree ごと回収される。
- **GC.compact / verify_compaction_references**(move は per-Ractor objspace と非互換): RLGC 時は non-move
  full GC にゲート ── commit b134827c5。
- **WB が freed slot に shared_bit をスタンプ(Layer-1)**: 別 objspace の生きた shareable が、global GC が
  回収した shareable(class / cc / cme)への WEAK 参照(subclasses imemo / inline-cache cc)を dangling させ、
  その死んだポインタが `b` として WB に届く。`-O3` は `GC_ASSERT(b != T_NONE)` を消すので stale shared_bit を
  スタンプ → 後で `gc_mark_shared_roots` が T_NONE を mark。WB の両 shared-bit 分岐を `RB_BUILTIN_TYPE(b) !=
  T_NONE` でガード(コンパイルアウトされたアサートのランタイム版)。→ maximize_global / longheld_slot_reuse が
  10+/10 → **0/10**。workflow で根因確証(9/20→0/50)。
- **cc-table が freed cc を強参照(cc-table cluster の近接機構)**: 計測で確定 ── `vm_cc_table_dup_i` の
  `memcpy(new_ccs, old_ccs)` が **同一 objspace 内**で「valid cme かつ **T_NONE cc**」の ccs を新テーブルに伝播
  (`RLGC-CCDUP` で確認; cur_os_cc/cme/tbl 全=1)。クラッシュは常に **global GC 中**(`local_gc=0,
  global_active=1`)で、cc は global GC が回収(confined GC は shareable cc を pin)。cc-table は再構築可能な
  キャッシュなので、**collected(T_NONE)cc/cme を持つ ccs は drop**(mark_cc_entry_i = invalidated-cme と同じ
  扱い+生存 sibling を invalidate、vm_cc_table_dup_i = コピーせずスキップ)。→ `VM/cc_table → T_NONE cc`
  アサートは解消、maximize_confined 12/12 → ~3-7/12。

**未解決の残存（極限ストレスでのみ顕在、通常〜型多様ワークロードは 0）**:
- **inline-cache cross-objspace dangling → SEGV(本系統の深部）**: cc-table アサートを潰すと、同じ dangling が
  **メソッド呼び出し時の SEGV** として顕在(fanin / gc_stress_everywhere 12/12, NULL deref)。shareable iseq の
  WEAK な `cd->cc`(iseq.c:391 `cc_is_active` で active のみ強マーク、それ以外は empty_cc にリセット)が、別
  objspace で global GC に回収された cc を指して dangling。キャッシュ層のガードでは塞げない(回収済み cc の
  inline-cache を mark 時に直せない)。**本筋の修正は §5.4 の cc/cme/cc_tbl の main objspace ルーティング**
  (cc の寿命を shareable iseq と一致させる)。Ractor 終了時の per-box classext→dead objspace ダングリング
  (§5.4 の objspace ハンドオフ)も同系。
- **`gc_mark_shared_roots` → T_NONE / out-of-heap(parent T_UNDEF)**: 直前 global GC で回収され page も解放
  された shareable を root が dangling 参照。Layer-1 で減ったが残存。同じ cross-objspace 寿命の根。
- **message :b 配列 → T_NONE 要素**(§3.10 残存, ~7.5%): 受信側 ractor_copy が並行 global GC 下で破損クローン。

**重要(再試行不要)**:
- **pre-existing**(cc/cme 修正の有無に関わらず発生 ── maximize_confined 2/5 w/o fix)。本セッションの修正は
  回帰ではない(btest_ractor 161/161、RLGC 有無とも)。
- **投機的修正は5連敗**: (a) cc_table マークに NON_BARRIER ロック → 効果なし。(b) free 時に shared_bit クリア
  → 効果なし(stale bit は free 経路起因でない)。(c) holder(§A.3) (d) don't-pin(§A.4)。(e) confined GC の
  foreign weak-ref を `handle_weak_references_alive_p` でスキップ(foreign は alive 扱い)→ **効果なし**(残存は
  弱参照解決でなく cc-table 強参照経路だった)。**rapid-patch では割れない**。計測駆動(free-ring で世代相関 +
  dup で伝播確認)が機能した。

**必要な対応**: 残るは cross-objspace **寿命**の構造課題(inline-cache cc / 終了 objspace の classext)。WB/
mark 層のガードでは閉じない。**§5.4 の main-objspace ルーティング(+ 終了 objspace ハンドオフ)が本筋**。

### 5.2 未監査で原理的に残るカテゴリ
1. **ユーザ定義 T_DATA の `dmark`/`dfree`** — confined ローカル GC 中に任意の C 拡張コードが走り任意の共有 C
   状態を触りうる(封じ込めモデルの根本的な穴)。要設計: custom-dmark を持つ T_DATA はローカル GC でマーク
   せずグローバル GC に委ねる等。
2. **JIT (YJIT/ZJIT)** — Rust 側の per-Ractor 相互作用・GC 外の共有表アクセスは未確認。
3. **`RUBY_INTERNAL_EVENT_FREEOBJ` フック** — confined sweep 中の発火で共有状態アクセス未追跡。

### 5.3 性能の follow-up
- グローバル GC の STW バリア(N=8 で ~13%)。major を「メモリ圧 or N 回ごと」だけグローバルにするスロットルは
  race 修正済みの現在なら再投入可。
- NON_BARRIER ロックを消す唯一の道: `generic_fields_tbl` を**並行ハッシュマップ**化(obj→fields の並行マップ
  インフラ新設が要る大作業)。

### 5.4 機能の follow-up
- **cc/cme/cc_tbl/shape-edge の main objspace ルーティング**(足場=`main_newobj_cache` は現状未配線・休眠で、
  `rb_gc_ractor_cache_alloc_on_main` も呼び出し元ゼロ。誤った前提で使われないよう注意)。これは §5.1 の本筋。
- **Ractor 終了時の objspace ハンドオフ/解放**: 現状 objspace は終了時にリーク。設計案「終了 Ractor の
  ローカルヒープは最初に join した Ractor が継承(no-move)」。
- **`GC.stat`/`GC.total_time`** の per-objspace 集計未実装。
- make_shareable したユーザ shareable はローカル objspace に pin-while-live、グローバル GC でのみ回収。

### 5.5 既知の制約
- `RGENGC_CHECK_MODE`/`check_rvalue_consistency` は RLGC 非対応(cross-objspace 参照を偽陽性で報告)→ RLGC
  デバッグには信用しない(`rlgc_obj_in_any_heap` で緩和済みだが完全ではない)。confinement の検証は §3.12 の
  AUDIT を使う。
- 保守的マーク中の "out-of-heap" 表示は「**現在の(driver)objspace に無い**」の意味(別 objspace の有効
  オブジェクトでも出る)であって「解放済み」ではない。

---

## 付録A. 検討して棄却した設計案

§5.1/§3.10 に至る過程で検討し、実証付きで棄却した案を記録(再検討の出発点として)。

### A.1 メッセージを受信側へ「単一 clone で直接」確保 — 不可能(トリレンマ)
**(I) スナップショット意味論**(コピーは send 時確定)/**(II) lock-free ローカル GC**/**(III) 単一トラバース
で受信側着地** の3つは同時に取れない:
- send 時にコピー(I) ⇒ 送信スレッドで走る ⇒ 受信側 objspace へ確保するには foreign allocation(受信側の
  無ロックヒープへ第2スレッドが書込)= **(II) 破壊**。確保先は暗黙(`rb_gc_get_objspace()`)で `#clone` の全
  newobj が暗黙先へ行くため「出力だけ」を向けるのも不可能。
- receive 時に単一 clone(III) ⇒ **(I) 破壊**(send 後の変更が漏れる)+ 受信キューが送信側の生きた
  unshareable を参照(より悪い)。
→ (I)+(II) を守る道は「2回目のトラバースを受信側で」= 採用した **materialize-on-receive**(§3.10)のみ。
`main_newobj_cache` の足場が安全な前例に見えるが、main は `local==FALSE`+CR ロック+STW でしか回収されない
=受信側ローカルヒープの真逆で、転用不可。

### A.2 「ウルトラC」: 送信スレッド T1 を一瞬 R2 に所属させる — 不可能
T1 の「現在 Ractor」を clone の間だけ R2 にすれば単一 clone で受信側着地、という案。検証済みキラー:
(1) objspace と newobj_cache は密結合で objspace だけ向けると元バグ再現+cross-heap 破壊; (2)「Ractor 1個
だけ止める」プリミティブが無い(唯一の停止=全 Ractor STW barrier=削除した直列化); (3) ローカル GC のルート
集合は駆動スレッドの EC に固定で、T1 駆動の R2 GC は R2 の生存ルートをマークせず UAF; (4) V1(全身分フリップ)
は user `#clone` が R2 身分で走り `Ractor.receive` がメッセージ窃取等; (5) 非同期 send が同期ランデブー化し
相互 send でデッドロック。境界の論拠: 本実装は recv_queue/ports だけ foreign-writer-safe にし、allocation
ヒープは意図的に lock-free・foreign-writer 非対応のまま — borrow-R2 はこの境界を侵犯する。

### A.3 materialize-on-receive の「holder」追補(再コピー窓の堅牢化) — 無効・撤回
§3.10 残存(再コピー中に global GC でピンが消え source が解放される窓)を、再コピー中だけ source を受信側
Ractor(shareable)から辿れる holder に載せて `gc_shared_relation` で再ピンする案。実装し 60回 A/B 比較 →
holder有 4/60 vs holder無 5/60 で**有意差なし(無効)** → 撤回・コード除去。残存は holder では閉じない
(§5.1 と同系統)。

### A.4 cc/cme を「shareable クラス限定」で pin — 悪化・撤回
§5.1 の cc/cme dangling を「unshareable クラスの cc/cme は pin しない」で直す案。実装し実測 → r4 が
**4/20→8/20 と悪化**(dangling する子が owner→def-body→inline-cache へ移るだけ)。トレードオフを動かすだけで
解決しない → 撤回(コメントに記録)。

### A.5 メッセージコピーの他案
- **B: コピーを main objspace に確保** — UAF は直るが in-flight+受信済みコピーが main に浮遊し global GC まで
  回収されず main 肥大・STW 頻発でコスト不可。
- **F1: 明示 export darray + global 照合** — 正しいが永続ルート集合・reconcile・多段転送・終了リークの追加
  機構が重い。
- **F2: shared_bits を cross-objspace エッジへ一般化** — root エッジが不可視(`parent_object=Qundef`、
  `gc_shared_relation` は `SPECIAL_CONST_P(parent)` で早期 return)で `basket_free` 後の root 参照を救えず
  **不完全**。
