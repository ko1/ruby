# RLGCv2 現状サマリ(2026-06-12)

設計の正典は `design_v2.md`。本書は「いまどこまで出来ていて、何が残っていて、どう検証するか」だけをまとめる。v1 の記録は `RACTOR_LOCAL_GC_DESIGN.md` / `RLGC_STATUS.md`(凍結)。

## 現在地

| マイルストーン | 状態 | 主要コミット |
|---|---|---|
| M0 土台(global objspace+page pool / cache 層撤去) | 完了 | 〜M0c |
| M1a per-Ractor objspace+封じ込め(STW 段階) | 完了 | 〜cbc3944d6 |
| M3 message send(受信側実体化・決定 11) | 完了 | d0affd097, 9fbe6af32 |
| M2 global GC(STW 一括・自動起動) | 完了 | d1f504b6e, 981664710 |
| M4 終了と引き継ぎ(value 併合 / orphan / shutdown) | 完了 | 1f8318f1f, b977ac426, 80f227d52 |
| **M1b local GC 並行化(バリア外し)** | **完了** | 2bc3fc1b6〜df4f2e0b3 |
| M5 堅牢化・調整 | 主要部完了 | e3bd4e939〜d2f855dcf |
| origin/master 追従 rebase(b765d9489 = upstream バンプポインタ・アロケータ) | 完了 | 全 36 コミット転写 |
| 決定 18: Ractor 宛て postponed job | 完了 | 独立ブランチ ractor-targeted-pjob 5f537434c(upstream 提案用)+ v2 へ cherry-pick 70cf4b6df |
| incremental marking × 単一→複数遷移の整合(設計 2.1 step 0 の未実装文) | 完了 | 730392475 |
| キュー2: orphan 併合の pjob 化(+ absorb GC 禁止ガード) | 完了 | ae6a8da90 |

すべてのコミットは full gate(`make btest` 2050 + `make test-all` 34892〜34904/0F/0E)を通過してから入れている。

## 性能(merge-base `aa4d4c450` 対照、10M iter 割り当て churn)

| | master | RLGCv2 | 比 |
|---|---|---|---|
| N=1 | 0.97s | 1.08–1.13s | 約 -11%(封じ込め税) |
| N=8 | 4.50s | 1.5–1.6s | **約 2.9 倍速**(実効 ~7.6 コア) |

このマイクロベンチはコードレイアウトで ±3–5% 揺れる。N=8 のスケーリングは自比 ~5.7×。

## ロック模型(M1b 後の最終形)

- worker の local GC: **無ロック**(封じ込め+atomic bitmap)
- main の local GC: **no-barrier VM lock**(VM グローバル root walk の保護)
- global GC: VM lock + barrier(`gc_enter_event_global`)
- **GC の内側では VM lock を取らない**(待機=バリア合流=半回収ヒープ露出)。GC 経路が触る VM 共有構造は専用 native mutex: registered globals / id2ref / generic fields(+ページプール固有 lock)。クリティカルセクションは「確保しない・ブロックしない」規律(確保が要る挿入は GC 禁止区間か二相)

## global GC の起動条件(§2.2、全 3 種実装済み)

1. shareable 増加: `shareable_objects > limit`(survivors×2.0、下限 1<<16)
2. 滞留: `stalled_shareables > limit/2` — mark 完了後の pin walk が数える「自 root から届かない shareable」(M5(7) で pin を gc_marks_finish へ移設し意味を厳密化)
2. zombie objspace 堆積 ≥ 8

## 検証手段

- **repro スイート**: `rlgc_repro/v2_*.rb`(自己完結 8 本 — mix / gen / fstring / clone-freeze /
  shutdown-flush + incremental×multi + orphan-pjob + **verify**(`v2_verify_consistency.rb`))
  + v1 オラクル `rlgc_repro/b7–b11`(65 本)。
- **RLGC 不変条件 verifier**: `GC.verify_internal_consistency` が s→u=shref 検査・
  shref⟹unshareable・bitmap⟺FL_SHAREABLE・T_NONE ビット衛生・封じ込め(u→外部 u 禁止、
  例外 box->top_self)・呼び出し Ractor の root スコープ(machine_context と設計上
  クロスルートな VM 大域は除外)を検査する。
  最終掃引: **ok 57 / timeout 8 / crash 0**(timeout は cpu≈wall の全力 spin = adversarial 設計、v1 期から master でも完走しない)
- **TSan**: worktree ビルド(`git worktree add` → clang-18 `-fsanitize=thread -O1`; in-tree srcdir 直は VPATH が in-tree .o を拾い破綻)。
  `TSAN_OPTIONS="suppressions=RLGC_DOC/tsan_suppressions.txt"` で**未分類 0**(suppression は全件根拠コメント付き; 非マッチ=新規=要調査)
- **ASAN**: 同 worktree 方式。mix/gen/g2/m42 バッテリ緑
- **ストレス**: `rlgc_repro/v2_concurrent_local_gc_mix.rb`(12 worker 並行 GC+終了/併合+global)を GC_STRESS / tiny-heap / stress+tiny の 3 条件でも緑

## 今日見つけて直した代表バグ(詳細は各コミットログ)

- M1b 系: Ractor dmark が他 Ractor の owner 変異構造を歩く(threads/EC ×1、queues/ports ×1)、deleted-key 機構の STW 前提、gc_enter の main 判定揺れ、interrupt queue の create→start 窓(v1 §6.4 残存面)、process-wide static の再書込
- M5 系: end_procs / trap_list の封じ込め漏れ、_id2ref build の単一 objspace 走査、**圧縮ガード未移植**(multi-objspace で full GC に degrade)、**value 継承物の到達性穴**(legacy/stdio/Thread wrapper → 併合直後 shref pin)、**mark_func_data redirect 乗っ取り**(v1 during_gc ゲート未移植 — 4 オラクル一括治癒)、**svar 封じ込め**(shref 不発 + Ractor 間共有 svar の per-EC 退避 = `$~` 漏れ解消)、**昇格カウンタの driver 偏り**(major ペーシング歪み)

## 残項目(2026-06-11 設計合意済みの実装キュー — 上から順に)

1. **mark_func_data の per-Ractor 化(§1.3 どおりへ)**: 現実装は VM 共有 + during_gc ゲート
   (M5(3))の暫定。per-Ractor 化でゲート自体を不要にする
3. compaction の global-STW 実装(§2.2 末尾に方針記載済み。当面は degrade のまま)
3. move の re-homing 方式(§4.4): コピー+無効化 vs dmove 特別扱い — ユーザ判断待ち
4. generic_fields の per-objspace 分割(§2.4-2): 性能最適化(現ベンチでは非ホット)
5. ASAN/TSan の CI 常設化(レシピ・suppression は完備)
6. N=1 の残オーバーヘッド(~11%)/ TSan watch: `VM_FORCE_WRITE` 単発(ペア未捕獲)
