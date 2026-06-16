# RLGCv2 — 10h 検証 campaign 所見と改修予想 (2026-06-15)

検証対象 = HEAD(コミット済み thgroup + clone 修正、move/rec は stash で除外)。
plain/ASAN/mega/btest/test-all = 実クラッシュ 0(test-all は test/ruby を verify 下で
~160 周、毎回 10770 tests / 0 failures)。以下は TSan が拾った **pre-existing な
M1b lock-free 並行面**の分類と改修予想。**2 修正のロジックは全 race の racing access に
一度も現れない**(clone は割り当て provenance に出るのみ)。

凡例: 重大度 = [REAL UB] 実害は薄いが未定義動作 / [BENIGN] 設計上無害 / [REAL?] 要オーナー確認。

---

## A. 修正済み・コミット済み
- `e344d556b` thgroup: Ractor の Thread が belongings 全体を root(`rb_thread_mark_owned_roots`)。
- `7c507438b` clone(freeze:): freeze_*_hash を register→CAS publish で race-free 化。

---

## B. IC(inline constant cache)族 — 約 470 report — [REAL UB], effect は概ね benign
`rb_vm_opt_getconstant_path` / `vm_ic_hit_p` / `vm_ic_update`。2 つの競合面:

1. **`ic->entry` ポインタの publish/load**: writer `RB_OBJ_WRITE(iseq,&ic->entry,ice)`
   (vm_ic_update:6555 → rb_obj_write gc.h:672) vs reader `ice = ic->entry`
   (getconstant_path:6566)。ポインタ語の非同期 load/store。
2. **entry スロットの read-after-reuse**: reader `ice->flags/ice->value`(vm_ic_hit_p:6528)
   vs writer `newobj_init` が**同一スロットを新オブジェクトとして初期化**。
   = reader の `ice` が解放→再利用されたスロットを指す = freed GC スロット read。

**重要な含意**: TSan が報告した = 2 アクセス間に STW barrier が無い(= happens-before 無し)。
shareable な IC entry は「global GC(STW)でのみ解放」のはずで、その間 reader は安全点で停止し
保守スキャンで entry が pin される想定。**それでも reuse が起きている**= entry の生存保証に穴が
ある(reader が保持中の entry が STW 非経由で解放/再利用される経路)。effect は薄い(再利用
スロットは hit 判定 false → cache miss → 再計算、誤定数値にはなりにくい)が UB。**ASAN 不可視**
(GC スロットは freelist 復帰で poison されない)。

**改修予想**:
- load を `RUBY_ATOMIC_VALUE_LOAD(ic->entry)` に(publish は既に RB_OBJ_WRITE)。
- 生存: (a) load 後の再検証 — `vm_ic_hit_p` 内/後で `IMEMO_TYPE_P(ice, imemo_constcache)` を
  再確認し、不一致なら miss 扱い(reuse を検出して捨てる、低コスト・hot path 安全)。
  (b) entry の解放を真に global STW のみへ限定できているか、free 経路を点検(根治)。
- 効率/リスク: hot path。まず (a) の防御的再検証で UB を無害化、(b) は機構解明後。
- 効果: 約 470 report の大半を解消。**唯一 effect が UB なので潰す価値が最も高い族**。

---

## C. Ractor 終了時の objspace 併合(absorb) — absorb:8202 vs gc_mark:5151 — 約 95 report — [REAL?]
`rlgc_objspace_absorb` が heap_page 構造(2648B)の `page->objspace` 等を書き換える(main が
mutex M0 保持)一方、別 Ractor の `gc_mark`(default.c:5151 の containment 判定
`GET_HEAP_OBJSPACE(obj) != objspace`)が同じフィールドを lock 無しで読む。

**判定**: 併合は §2.3「join した者がその場で併合」。書き側は **mutex 止まりで full barrier では
ない**ように見え、その間に他 Ractor の local GC が page 所有を読む。読む値が併合前/後で
containment 判定が変わる → 該当 object の mark をスキップ(早期回収)or 異 objspace bitmap 書き
の可能性。**設計が absorb を STW 想定なら、それが効いていない実バグ。**

**改修予想**:
- `objspace_absorb` を global STW barrier 下で実行(全 Ractor 停止、§2.3 の意図通り)。
- または `page->objspace` を atomic 化 + 「どちらの値でも安全」を保証(併合中の object は
  どちらの objspace から見ても leaf なら可)。
- リスク: 終了/join 経路。要オーナー確認(設計意図 = STW か否か)。

---

## D. fstring / concurrent_set の cross-objspace bitmap 読み — 約 250 report — [REAL UB] だが pin で実害なし
`rb_concurrent_set_find` → `rb_gc_impl_garbage_object_p`(default.c:1986) が**他 Ractor 所有
ページ**の `flags.before_sweep`(gc_sweep_start_heap:4338 が書く)+ mark bit(RVALUE_MARKED:1605)
を読む。関連: gc_sweep_page(11)/gc_pin:5191(7)/RVALUE_REMEMBERED(1)。

**判定**: fstring 表エントリは born-shareable で pin、実際には解放されない → racy bit は
「not garbage」の判定を変えない。**effect は benign**、ただし freed/sweeping 中ページの
flags を cross-objspace で読むのは UB。

**改修予想(最もきれいな実改善)**:
- `rb_concurrent_set_find` の garbage チェックを **shareable/pin 済みエントリでは短絡**
  (shareable は定義上 live → `garbage_object_p` を呼ばない)。cross-objspace bitmap 読みが
  消える。低リスク・低工数で約 250 report を解消。
- 効率/リスク: 低。fstring/sym 表に限定。

---

## E. call cache 派生 — vm_cc_bf_set vs vm_call_single_noarg_leaf_builtin — 25 report — [BENIGN]
`vm_cc_bf_set`(cc の builtin 関数ポインタ書き)vs 別 Ractor の builtin 呼び read。
suppressions 既載 `race:vm_cc_call_set` と**同族**(同じ cme に同じ値を書く・old/new 両 valid・
生存は born-shareable pin)。

**改修予想**: `race:vm_cc_bf_set` を tsan_suppressions.txt に追記。trivial。

---

## F. postponed_job_queue — flush:1984(非 atomic read)vs preregister(atomic exchange) — 約 13 — [BENIGN, atomic 不整合]
新 objspace 初期化(Ractor 生成)で `rb_postponed_job_preregister` が atomic 書き、稼働中
Ractor の `rb_postponed_job_flush` が同 global を非 atomic read。

**改修予想**: flush 側の当該 read を `RUBY_ATOMIC_*_LOAD` に(writer は既に atomic)。
または preregister を boot 時に全枠確保。低工数。

---

## G. keyword_ids 遅延 ID 初期化 — rb_get_freeze_opt:460 — 7 — [BENIGN, 冪等]
`static ID keyword_ids[1]; if(!keyword_ids[0]) CONST_ID(...,"freeze");`。並列 Ractor が同じ
intern ID を書くので冪等・無害。Ruby C 全域に多数ある idiom。

**改修予想**: suppress(family)or 起動時 eager `CONST_ID`。trivial。clone 修正(freeze_hash)
とは別 static(freeze 引数パース側)で無関係。

---

## H. TSan 以外の未決(handoff 由来、campaign 非検出)
- **move/rec(現 stash)**: campaign 対象外。戻して独自検証パス(send/move の deep graph)を
  通してから commit(#18)。
- **C-1 send-copy remember 漏れ**: shape_edges では誤帰属(真因=thgroup)と判明し棚卸し済。
  別個に実在するかは **deep generic-ivar グラフの send-copy 専用オラクル**で確認推奨
  (現オラクル群は send-copy を強く叩いていない)。
- **generic_fields per-objspace 分割**: 現状 global 表 + lock(§2.4-2 未達)。性能/競合面。
- **compaction global-STW 化** / **N=1 税(~11%)**: ロードマップ。

---

## I. 推奨着手順(リスク低→高、効果大優先)
1. **D**(shareable 短絡): 低リスク・約250 report 解消・実 UB 除去。
2. **E, G**(suppress 追記): trivial。
3. **F**(flush の atomic load): 低工数。
4. **C**(absorb の STW 化): 設計意図確認の上。
5. **B**(IC): hot path。まず防御的再検証(B-a)で UB 無害化、機構根治(B-b)は別途。

---

## 実施結果 (2026-06-15) — 6 コミット、v2 スイート TSan クリーン化

| 項目 | 対応 | コミット |
|---|---|---|
| (既) thgroup mark 漏れ | fix | e344d556b |
| (既) clone(freeze:) race | fix | 7c507438b |
| **D** fstring/concurrent_set cross-objspace bitmap | **fix**(containment: foreign は非garbage) | a20f6607d |
| **F** postponed_job atomic 不整合 | **fix**(reader を atomic load) | 599f11ac1 |
| **G** keyword_ids 遅延ID | **fix**(idFreeze 直接使用) | d341d5c3f |
| **B** 定数IC | **benign 確定→suppress**(born-shareable+STW バリア同期、TSan 不可視の happens-before) | 58ff16e00 |
| **C** absorb | **benign 確定→suppress**(marker≠src,dst→foreign 判定不変) | 58ff16e00 |
| **E** call cache (bf_set) | **benign→suppress**(vm_cc_call_set sibling) | 58ff16e00 |

検証: D/F/G 各々 ASAN 0 + btest + suite green、当該 race 族の消滅を TSan で確認。
最終: **8R×10oracle×stress で未抑制 TSan race = 0**、btest 2049、ASAN 0、v2 suite 10/10。

B の当初「real UB」評価は撤回(born-shareable + global GC バリアが reader を待つので freed read は
起きない。詳細はメモリ rlgc-v2-tsan-ic-uaf)。

### 未対応(別タスク)
- **move/rec**(handoff #18): working tree に復元済み、独自検証後コミット。
- **C-1 send-copy**: shape_edges では誤帰属と判明。実在確認は deep generic-ivar send オラクル推奨。
- generic_fields per-objspace 分割 / compaction global-STW / N=1 税: ロードマップ。

## 2026-06-16 OPEN: move-courier SEGV, TSan+stress only

`v2_move_rehome.rb:70` / `v2_move_churn.rb:21` SEGV (`memcpy(dest=NULL,
src=<stack 0x7fff...>, size≈-1)`) while the receiver walks a just-received
moved graph -- `m` is a corrupt/freed Array.

Characterisation:
- MOVE-ONLY: ~12 instances across rehome/churn/edge; ZERO on any copy oracle.
  So it is in the off-heap move path, not a general RLGCv2 race.
- TSan+stress ONLY: never on plain (~45k runs) nor ASAN (clean). Timing race.
- Invisible to both sanitizers as a memory error: a freed GC slot returns to
  the freelist (not malloc-freed), so neither TSan (no data-race report with
  suppressions) nor ASAN (no poison) flags it. It surfaces only as a downstream
  SEGV when the reused slot holds incompatible data.
- A graph node is collected though logically reachable -> a GC-lifetime bug.

Tried, did NOT fix: RB_GC_GUARD(result) in ractor_basket_value to root the
materialized graph across ractor_move_courier_free/reset_belonging (the result
otherwise lived only in the malloc'd basket's p.v). Kept as a defensive
improvement; the SEGV persists -> root cause is elsewhere (build/materialize,
or a containment edge, or a pre-existing GC race the move pattern triggers).

Localization blocked: Ruby's [BUG] C-backtrace is truncated under TSan; cores
go to apport (no sudo to redirect core_pattern); gdb perturbs the timing race
away; an LD_PRELOAD sigaction shim disabled TSan's own handler too.

Next ideas: a dedicated debug build that (a) keeps a C backtrace under TSan, or
(b) asserts graph integrity at receive-return vs walk-time to bisect the
window; or instrument newobj/sweep to log when a known in-flight node's slot is
freed.
