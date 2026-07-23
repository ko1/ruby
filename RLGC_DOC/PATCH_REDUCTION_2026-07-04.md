# RLGCv2 パッチ縮小プラン (2026-07-04)

目的: rlgc-v2 の diff を小さく・簡単にする。
(1) 実装の簡単化、(2) master へ先行投入できる部分の切り出し。

前提: `REVIEW_2026-07-04.md`(徹底レビュー)の後続分析。分析時点の HEAD は
レビュー対象 6fb5cc9dc より進んでおり、**A-1/A-2/A-3/A-7/C-2/B-1/B-3/B-4 は修正済み**
(REVIEW の「対応状況」参照)。本プランはそれを織り込み済み。

規模の現状: diff ≈ +10,924 / −844(66 files)。うち **54%(5,885 行)は
RLGC_DOC/ + rlgc_repro/**。実コードは ≈ +5,040 行
(default.c +1,995 / ractor.c +967 / gc.c +642 / ractor_sync.c +366 / variable.c +337)。

関連する訂正: **B-14(fork × zombie_threads leak)はおそらく誤報**。upstream は
`rb_thread_atfork_internal` → `rb_vm_living_threads_init` が zombie_threads を fork 時に
再初期化しており clean(REVIEW の B-14 に訂正注記済み)。

---

## 1. 簡単化(優先順)

### S-1. RLGC_DOC/ と rlgc_repro/ を review diff から分離 — △5,885 行・リスクゼロ
- diff の 54% が docs(4,864 行、TSan suppressions 534 行含む)+ repro(1,021 行)。
- 方法: (a) companion branch(例 `rlgc-v2-doc`)に分離、または
  (b) レビュー依頼を `git diff -- ':!RLGC_DOC' ':!rlgc_repro'` で提示。
- TSan suppression ファイルは特にコード diff に置くべきでない(開発ツーリング)。

### S-2. registered globals の per-Ractor 分割を設計原案(VM 単一リスト)に戻す — △150〜200 行
- design §2.1 3.e は「登録スロットに後から別 objspace の値が入り得る」ことを根拠に
  **per-objspace 分割を明示的に棄却**していたのに、3a875ac30/2604ef095 が分割を実装し、
  予言どおりの穴が開いた(B-1: cross-Ractor unregister no-op → 50a17d068 で観測罠つき
  暫定対応済み / B-2: worker 値が main 登録スロット経由で誰の root にもならない —
  こちらは**未解決の設計判断事項**)。
- 戻すと消えるもの(≈160 行): ractor.c:329-430 の登録機構、ractor_core.h の
  fields/decls、**zombie-owner 特例 b28fb45a5(+86/−29)**、帳簿の owner 配管、
  ractor_free の移送呼び出し。加えて vm.c で削った upstream の
  `global_object_list`/`mark_object_ary` 76 行が復活し diff がさらに縮む。
- 同期は実績のある `registered_globals_lock` を復活(2604ef095 が消したもの。
  VM→reg のロック順序問題は 2ffd2b01e で解決済みの形をそのまま使う)。
  register/unregister は cold path。
- 失うもの: local GC hot path からの「O(全登録数) 走査 + 共有 lock」除去。
  設計自身が「登録物は定数規模」(チューニングは M5)と許容している。
- **修正(B-2 の構造的解消)と簡単化と設計準拠が一致する、最レバレッジの 1 手**。
  B-1 の観測罠(50a17d068)もリスト一本化で不要になる。

### S-3. `last_cycle_pinned` 機構の削除(A-3 修正 327e573c2 の後続)
- A-3 修正で stillborn objspace が失敗時点(VM lock 下・sweep 外)で処理されるなら、
  「sweep 内からの帳簿 push が単一 objspace 判定を mid-sweep で反転させる」ケースが
  消え、そのためだけに存在する `rlgc.last_cycle_pinned`
  (field default.c:638 / set 6689-6691 / reset 8597 / read 4733)は
  live 述語(`!rb_gc_single_objspace_p() && !during_global_gc`)への置換で削除できる。
- 前提: 失敗パスの捕捉範囲が `rb_thread_alloc`(thread.c:1085)の NoMemory も含むこと
  (半端に sweep-push 経路が残ると backstop を消せない)。327e573c2 の捕捉範囲を確認。
- 効果: △4 行(コード)+ △25 行(難解なコメント)+ **「cycle 束縛 vs world 束縛」
  という非局所不変条件が 1 個消える**(行数以上にレビュー可能性が上がる)。

### S-4. T_FILE の move を当面エラー化 — △30 行 + A-4(production UAF)解消
**【supersede 注記】** その後の修正バッチ(tip c2170bbf3)で A-4 は「fptr VALUE の
courier 子ノード化」により正攻法で修正済み。**この項は不要になった**(エラー化より
+40 行だが健全な形が既に入った)。記録として残す。
- courier の IO 枝(enum/union ractor.c:2368/2393、capture 2636-2646、
  materialize 2711-2715、mark 2837-2839)を削除すると、T_FILE は既存の
  `default: rb_raise("can not move a ...")`(ractor.c:2648)に自然に落ちる。追加コード不要。
- 失うもの: master の move は IO を支持(slot memcpy)しており退行。ただし
  upstream の btest/test-all に IO move のテストは無いことを確認済み。
  健全な実装(fptr の VALUE 6 個の courier ノード化)は逆に +40 行なので、
  後日独立コミットで再導入するのが筋。
- 注意: **MatchData move は削ってはいけない**(§2-B 参照)。

### S-5. modular GC(mmtk)をブランチでは明示無効化 — △44 行 + B-12 解消
- 現状はどのみちロード不能(dlsym 必須の `rb_gc_impl_gc_rest` / `each_objects_shareable`
  が mmtk.c に無い)で、mmtk.c +47 行の過半は到達不能。さらに 5 関数
  (unpin_in_flight_message / during_global_gc_p / shref_marked_p / heap_page_count /
  objspace_absorb)はテーブル外で default 実装へ誤ルート。
- `#if USE_MODULAR_GC` → `#error "RLGCv2 does not yet support modular GC"` の ~3 行に置換。
  正式対応は後日 +30 行で可能。

### S-6. `stalled_shareables` トリガ(§2.2 条件 2)の削除を検討 — △15 行
- B-3(リセット漏れ)は 7784a9702 で修正済みなので緊急性は下がったが、
  この指標は設計意図(「新たに mark された shareable 数」)に対し
  shref traverse 分を含む過大計数のままで、トリガとしての品質が低い。
- 当面はトリガ 1(shareable 数 + limit 再計算 — これは load-bearing、維持)+
  トリガ 3(zombie ページ)+ 明示で足りる。条件 2 が守る「全 Ractor が一様に
  2 倍未満ずつ育つ」ケースは狭い tail。必要になったら「正しくリセットされる指標」
  として独立コミットで再設計。
- 関連: `shareable_objects` に decrement サイトが無い(単一 objspace 世界の sweep で
  shareable が死んでも減らない → multi 移行直後に余計な global GC 1 発)。
  free 時 bit clear と同時に decrement するヘルパ(`rlgc_shareable_count_adjust`)で
  一元化するのが良い。

### S-7. dead/debug コード削除 — △75 行
- `rb_replace_generic_ivar`(variable.c:2472): 唯一の呼び手(旧 move 経路)が courier で
  消滅したのに ~30 行かけて書き直されている。関数ごと削除。
- verifier 内の soak-hunt スキャフォールド(default.c:~5988-6030、~40 行):
  `/proc/self/mem` を開いて stale 対象を pread し、Thread フィールドをポインタ同値で
  同定して、エラーを握りつぶす。plain な `err_count++` 報告に縮約(直下の封じ込め
  チェックが本命のアサーション)。
- 細片: default.c:1315 の `RUBY_DEBUG_LOG("heap alloc_using_page...")`、
  gc.c:411 の `// TODO: ce->file should be shareable?`(解決するか消す)。

### S-8. zombie wrapper pin は現 HEAD(30a886470 = STW 限定 reap)のまま維持
- 3 案比較の結論: (a) leaf lock 復活 ≈ 90 行の churn + confined GC が VM-global list を
  再び iterate、(c) atomic 化は hot WB path 4 箇所(default.c:7465/7519/7531/7550)に
  波及し「writer を 1 個見落とせば lost-update」の脆さが残る。
  (b) reap の gate(2-3 行)が最小で、コストは finished zombie が 1 cycle 余分に
  残るだけ。**これ以上触らない**。

### 簡単化に見えるがやらない方がよいもの
1. **id2ref confinement(fbf838815)の巻き戻し**: 測定の結果、confinement は +34/−76 で
   **lock 版に戻す方が +42 行大きい**。必要なのはコードの変更ではなく
   「非 main `_id2ref` = RangeError」という仕様変更の記録(doc 課題 2)。
2. **MatchData move の削除(△105 行に見える)**: upstream bootstraptest が MatchData move
   の round-trip を assert しており(origin/master btest test_ractor.rb:2043)、
   エラー化は upstream テストを割る。Marshal 不可・copy 代替は move 意味論を壊す。
   re.c ヘルパは courier で最も健全な部分でもある。
3. **generic_fields per-Ractor 分割の巻き戻し**: これは registered globals と逆で、
   design §2.4 分類 2 が指示する形どおり。hot path(ivar)から global lock を除去した
   本丸で、発見済み 4 バグは修正・検証済み、残る 3 件(A-5 / B-10 / B-11)は
   各 ≤10 行の修正。戻すと hot path lock が復活し設計にも反する。
4. **pinned-roots walk / shref 機構 / `rlgc_global_gc`(~215 行)/ zombie 帳簿 / courier 本体**:
   設計の核。local/global の重複も予想より少ない(root 表共有の規律は実装されている)。
5. **verifier 追加(containment チェック等)**: WB-miss 級のバグ検出の実績があり、
   CHECK gate 済みか `verify_internal_consistency` 内。S-7 のスキャフォールドだけ削る。
6. **`rb_objspace_each_objects_local` への caller リネーム(ext/, jit.c, coverage.c)**:
   見た目 churn だが実体は API 移行(current-Ractor-only walk への振り分け)。維持。

---

## 2. master に先行投入できるもの(推奨順)

パターンは実績済み 3 件(each_objects barrier e5518bee2 / iseq loader WB db3a1939f /
per-Ractor pjob e70011179)と同じ:「master 単体で正当化できる硬化・修正・仕様確定」。

### Tier 1 — 純バグ修正(仕様議論不要・すぐ PR 可)

1. **`check_rvalue_consistency_force` の empty_page 無限ループ**(1-2 行、リスク S)
   - origin/master gc/default/default.c:1510-1520。`empty_page = empty_page->free_next`
     欠落。`GC.verify_internal_consistency`(release でも呼べる)から到達可能。
   - **rlgc-v2 側も未修正**なので upstream 修正 → rebase で拾うのが最安。
2. **missing/dtoa.c `pow5mult` の読み手側 race**(~6-12 行、リスク S)
   - publish 側は既に `ATOMIC_PTR_CAS`(b_cache)なので、残る欠陥は `p5s`/`p5->next` の
     plain load(dtoa.c:887/897)のみ。`ATOMIC_PTR_LOAD` 化 + `#ifndef` fallback。
     lock 不要・性能コストなし。branch の TSan suppression が 1 本消える。
     既知の upstream Ractor バグ([[rlgc-v2-upstream-dtoa-not-ractor-safe]])の正式修正。
3. **MN scheduler: `sched.finished` の早期 publish = stock master の teardown UAF 窓**
   (~30 行、リスク M)
   - **重要な認識の逆転**: branch の dying_th handshake(4de030fda)は「バグ」ではなく
     「fix」。upstream thread_pthread_mn.c:492/501/506 は terminal な
     `coroutine_transfer0` の**前**に finished を立てるため、barrier 外の GC が
     観測 → delist → `rb_threadptr_sched_free` が使用中の context/stack を解放し得る。
   - 抽出内容 = 4de030fda(+17 thread_pthread.c / +9 thread_pthread.h / mn.c 3 行)。
     RLGC 非依存、コメントの書き換えのみ必要。厳密には finished の store/load を
     atomic release/acquire にする(REVIEW B-7)のも同 PR で。
4. **`fiber_memsize` の thread_ptr deref 除去**(~12 行、リスク S)
   - `fiber != th->root_fiber` → `fiber->first_proc != 0`。master でも挙動同一で、
     dying fiber への `ObjectSpace.memsize_of` が頑健になる。

### Tier 2 — 仕様変更を master で先に確定(RLGC の前提を master の不変条件へ)

5. **at_exit/END 非 main Ractor = エラー**(~10 行 + テスト、リスク S)
   - **Matz 合意済み(2026-06-30)・両ツリー未実装**。master 今日でも:
     2 Ractor の `rb_set_end_proc` prepend は無同期(data race)、shutdown は worker の
     unshareable proc を main で実行(isolation hole)— 単独で正当化可能。
   - `rb_f_at_exit`(eval_jump.c)と `m_core_set_postexe`(vm.c)に
     `rb_ractor_main_p()` ガード。公開 C API `rb_set_end_proc` は維持(random.c 等)。
   - RLGC 側は end_procs 封じ込め項目 + B-5(weak-memory publish)が丸ごと消える。
     **行数あたりの効果が最大**。
6. **`rb_backtrace_dup` を master の Ractor send に導入(stage 1: passthrough は残す)**
   (~31 行、リスク S)
   - master 今日でも: passthrough された共有 backtrace T_DATA の `strary`/`locary`
     lazy 実体化が 2 Ractor で race。dup(frame memcpy + RB_OBJ_WRITTEN、lazy 配列は
     未設定のまま)は identity 以外挙動保存。
   - RLGC diff から vm_backtrace.c +23 と ractor.c の該当部が消え、#8 の前提になる。
7. **`ObjectSpace._id2ref` の main Ractor 限定**(~7 行、リスク S)
   - master は既に `_id2ref` deprecated + multi-Ractor では非 main の unshareable 解決を
     RangeError 化済み。残る差は「shareable id の非 main 解決」だけで、deprecation に
     覆われた小さな tightening。
   - これが master に入ると RLGC の id2ref 差分は「master と同じ」に縮退する。
8. **決定 11 シリーズ: send-copy は user `#clone`/`initialize_copy` を呼ばない**
   (native shallow copy + Marshal fallback + passthrough 廃止 stage 2)
   (~140-170 行 + テスト ~40 行、要 Feature チケット、リスク M(機械的には S))
   - master 今日: unshareable ノードごとに `rb_funcall(obj, idClone)` — ユーザコードが
     半コピーのグラフを見ながら VM に再入する。`ractor_native_shallow_copy`
     (ractor.c:2851-2912)は **RLGC 依存ゼロ**を確認済みで、master の単一 traverse に
     コピー関数だけ差し替えられる。
   - user-visible 変更: copy フック不発火 / 非コア型は Marshal 意味論 /
     singleton 持ちは送信不可 / エラーメッセージ変更 — branch の
     bootstraptest/test_ractor.rb・test/ruby/test_ractor.rb の変更はこれと**一緒に**
     upstream へ(先行不可)。
   - **RLGC 差分縮小の最大単品**であり、いちばん重い仕様議論を RLGC 本体レビューの
     前に済ませられる。RLGC はこれ無しでは成立しない(clone ベース copy は CoW buffer
     root を objspace 跨ぎで共有する)。
9. **define_finalizer 制限(決定 12)**(△58 行、要チケット、リスク S)
   - master には Ractor チェックが皆無(worker が `define_finalizer(String, proc{})` →
     unshareable proc が main で走る isolation hole)。master 版ルールは
     「shareable 対象 ⇒ main のみ」となり branch よりやや強いが、これが実は
     REVIEW B-9(own-objspace shareable の穴)も同時に解決する。RLGC は後で緩められる。

### Tier 3 — 機械的 prep(単独価値低、シリーズに同乗)

10. `rb_thread_mark_owned_roots` の抽出(~55 行)— **注意**: branch 版は `thread_mark`
    から `rb_gc_mark(rb_ractor_self(th->ractor))` も削るが、この辺は master では
    load-bearing(zombie 窓の Ractor 埋込 sched lock の延命)。中立 PR は self mark を
    残して抽出のみ。
11. `rb_objspace_each_objects_local` alias + caller 5 箇所リネーム(~30 行)—
    master では no-op。API 分割の概念を先に飲んでもらう必要あり。
12. `rb_match_init_copy` export(~8 行)— 単独では dead export。#8 シリーズの先頭
    コミットとして。

### 抽出不可と確定したもの(時間を使わない)
- **off-heap move courier 全体(~650 行)**: RLGC シンボル依存はゼロだが、単一ヒープでは
  in-flight snapshot が既に GC root で master の move は既に zero-copy memcpy —
  存在しない問題を解きつつ **master には退行**(object_id 連続性喪失
  (`rb_gc_obj_id_moved` 不使用)、String/Array/Hash サブクラス落ち(B-8)、
  対応型の縮小)。RLGC シリーズと一緒に出す。
- gc/gc.h の削除 2 件(`rb_gc_cr_lock` / `rb_gc_ractor_newobj_cache_foreach`):
  master の default.c がまだ使用中。
- `rb_gc_checking_shareable()` ガード除去(imemo.c/gc.c): master の現行
  shareable-verify 意味論の一部。RLGC は shref 記録で置き換えるから消せるだけ。
- `rb_gc_pointer_to_heap_p` の再追加: upstream が意図的に消した API の復活 = 方針逆行。
- gc_impl.h の新 entry points: `rb_gc_impl_gc_rest` 以外は RLGC 形。単独 PR の価値なし。
- thread.c の interrupt-queue hunk: 既に upstream(branch はコメント書換のみ)。
- テスト変更: 決定 11/12 の新意味論をエンコードしており #8/#9 と一緒に。
- vm_trace.c の GET_RACTOR() gate、cont.c fiber-pool leaf lock: RLGC 消費者なしでは
  no-op/dead(pool lock hunk は assert 削除を同梱しており単独では出せない)。

---

## 3. 合算効果と推奨順序

- 簡単化(S-2〜S-7): コード diff **△330〜400 行**、同時に A-4 / B-2(構造的)/
  B-12 が解消、b28fb45a5 特例と last_cycle_pinned 不変条件が消滅
- 先行 PR(Tier 1-3): さらに **△400〜450 行** + テスト移管(最大単品は決定 11 の ~180)
- S-1(docs/repro 分離): review 対象 **△5,885 行**

→ コード diff は実質 ~5,000 → **~4,200 行**。消える部分はレビューで特定した
2 つの弱点面(ロック撤去の plain-store 論証・異常系)にちょうど重なる。

**推奨順序**:
1. S-3(last_cycle_pinned 削除 — A-3 修正済みなので今すぐ可能)
2. S-2(registered globals 単一リスト復帰 — B-2 の設計判断ごと解決)
3. Tier 1 の 4 本(純バグ修正、並行して PR 可)
4. #5 at_exit エラー化(チケットは形式的)
5. #6 backtrace dup → #7 _id2ref
6. #12 → #8 決定 11 シリーズ(Feature チケット、テスト同梱)
7. #9 finalizer チケット、S-4/S-5/S-6/S-7 は空き時間に
8. Tier 3 は各シリーズのライダーとして

依存関係: #12 → #8、#6 は #8 より先(#8 が passthrough 問題を包含)、
#10 は self mark 維持が条件、S-3 は 327e573c2 の捕捉範囲確認が前提。

---

*生成: Claude Code(Fable 5)、簡単化分析 + upstream 抽出分析の 2 エージェント +
本体統合、2026-07-04。静的解析(ビルド・実行なし)。行数はいずれも概算。*

## 3. S-3 / S-6 の検討結果(2026-07-23)

### S-3(last_cycle_pinned 削除) — 却下
live 述語 `!rb_gc_single_objspace_p() && !during_global_gc` への置換は不可。
single→multi の mid-sweep 反転源は「sweep 内からの帳簿 push」(A-3 で解消)だけでなく:
1. creator の lazy sweep 途中の `Ractor.new` — 生成経路は creator の進行中 sweep を
   畳まないので、`creating_child_objspace`/`ractor.cnt` の変化で述語が即反転する。
2. 他 Ractor の非同期終了による zombie objspace push — 任意 objspace の lazy sweep
   と並行に起こる。
反転すると CHECK の pinned-free assert が「single-world mark が正当に unmark のまま
残した死 shareable」へ誤発火(rb_bug)する。assert を mark 時点の世界に束縛する
`last_cycle_pinned`(1 byte + 約10行)は本質的に必要。

### S-6(stalled_shareables トリガ削除) — 却下
削除動機だった「一度 retention trigger を踏むと指標が下がらず永久 STW」は、global
サイクルでの 0 リセット(default.c gc_start global 分岐)で修正済み。トリガ自体は
設計 §2.2 条件2 の安全網で、条件1(per-objspace 2倍)が発火しない「全 Ractor が一様に
limit 未満で育つ」ケースの shareable garbage 無限滞留を拾う唯一の機構。計数は
pinned walk の marked_slots 差分で追加コスト無し。削除は unbounded retention の再導入。
