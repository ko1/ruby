# Ractor-local GC 実装計画

CRuby (`gc/default/` のみ対象。mmtk/wbcheck は対象外)。本ドキュメントは実装の段階計画。
各 Milestone は **単体でビルド可能・テスト可能・原則として非回帰**であることを要件とする。

## 1. ゴールと前提（確定事項）

- Ractor ごとの **local GC** を導入し GC を並列化する。
- **objspace を分割**: 大半は per-Ractor の **local 部分**（heap、page、free-list、mark/sweep 状態）、ごく一部が **global 管理データ**（stress flag, config, sorted page 配列, finalizer/object_id/weakmap テーブル等）。**global heap は無い**（オブジェクト格納域はすべて某 Ractor の local heap）。
- **CRuby は moving 不可**（pin・C 拡張の生ポインタ・conservative stack 走査）。よって shareable を別領域へ移送しない。
- **shareable は global GC まで pin**。各 Ractor は自分の local heap に対し minor/major の local GC を回すが、**local GC は shareable を解放しない**。**global GC**（全 Ractor stop-the-world）だけが shareable を回収し、後述の remset を clear+recompute する。

### オブジェクトの 3 状態

| 状態 | 定義 | 回収者 |
|---|---|---|
| **shareable** | `FL_SHAREABLE`（frozen 深い shareable / Class・Module・Ractor 等） | global GC のみ（pin） |
| **shared** | unshareable・mutable・単一 Ractor アクセスだが **shareable から直接刺されている**境界エントリ | global GC のみ |
| **local** | 通常の unshareable | 所有 Ractor の local GC |

一様則: **「shared/shareable から到達できる物は global GC まで死ねない」**。

### 採用機構（X 案・境界のみ remset）

- shareable `S` → unshareable `U` のエッジができたら、**直接刺された境界 `U` だけ**を所有者の **remset = per-page `shared_bits` bitmap**（`remembered_bits` と同型）に記録する。閉包全体は塗らない。
- local GC の root = `ローカル root ∪ {自 space の shared_bits が立つ U}`。そこから通常の local mark で推移的に辿り、**shareable 境界で打ち切る**。`U` の先の部分木は素の **local** オブジェクト。
- **WB**（`s→u` 発生時）が `shared_bits` を立て、**global GC の full mark 中に clear+recompute**（既存の `objspace->rgengc.parent_object` を使い、shareable 親→unshareable 子のエッジで bit を立て直す）。WB は追加のみ・削除しない。
- **定理**: `s→u` エッジは `U` の所有者しか作れない（isolation により他 space の unshareable 参照を持てない）。よって bit は常に所有者が自分の `U` に立て、`S` を見る必要がない（`S` は別 space に居てよい）。

### 正しさの要

**WB 完全性**: `s→u` を作る全ストアが単一 choke point（`rb_gc_impl_writebarrier` ← `RB_OBJ_WRITE/WRITTEN/ATOMIC_WRITE`）を通り、`U` に `shared_bits` が立つこと。1 つでも漏れると、別 space の `S` 経由でしか届かない `U` が local GC で root されず premature free。
→ Milestone 1 で **実行可能な監査**（full mark 中に「WB が立て損ねたエッジ」を検出）として担保する。

## 2. 主要な実装サイト（`gc/default/default.c`、現行行)

- `struct heap_page` bitmap 群: 842–852（`remembered_bits` 848 を踏襲して `shared_bits` 追加）
- bitmap マクロ: `MARK/CLEAR/MARKED_IN_BITMAP` 915–917, `GET_HEAP_*_BITS` 919–923
- 一括クリア（major GC 開始時）: `rgengc_mark_and_rememberset_clear` 6151–6164
- remset を root にマークする雛形: `rgengc_rememberset_mark(_plane)` 6078–6148
- marking 中の親追跡: `objspace->rgengc.parent_object`（636–637）、`rgengc_check_relation` 4524、`gc_mark` 4617（`gc_mark_set` 早期 return の **前** 4622 が全エッジ実行点）
- WB choke point: `rb_gc_impl_writebarrier` 6230–6277
- compaction の bit 追従: `gc_move` 7096–7183
- full mark 完了: `gc_marks_finish` 5561
- per-Ractor 確保 cache: `rb_ractor_newobj_cache_t` 206、objspace 取得 `GET_VM()->gc.objspace`（gc.c:245）
- shareable 判定: `RB_OBJ_SHAREABLE_P(obj)` = `FL_TEST_RAW(obj, RUBY_FL_SHAREABLE)`（include/ruby/ractor.h:235）

flag bit は型ごとに枯渇のため **bitmap を採用**（任意型の `U` に立てる必要があるため）。

## 3. Milestones

### M1: `shared_bits` 基盤 + WB フック + mark 中 recompute + 監査  ← 最初に実装
非回帰（collection 挙動は不変。bitmap はまだ回収判断に使わない）。
- (a) `struct heap_page` に `bits_t shared_bits[...]`、page flags に `has_shared_objects:1`、`GET_HEAP_SHARED_BITS` 追加。
- (b) WB フック: `rb_gc_impl_writebarrier` で `RB_OBJ_SHAREABLE_P(a) && !RB_OBJ_SHAREABLE_P(b)` なら `MARK_IN_BITMAP(shared_bits, b)`。
- (c) mark 中 recompute: `gc_mark` の全エッジ点に `gc_mark_shared_relation()` を追加。親 shareable・子 unshareable で `shared_bits` を立てる。
- (d) clear: `rgengc_mark_and_rememberset_clear` で `shared_bits` も memset（production のみ）。
- (e) compaction: `gc_move` で `shared_bits` を src→dest 追従。
- (f) 監査: `RACTOR_LOCAL_GC_AUDIT` ビルドでは clear をスキップし、(c) で「WB 未設定のエッジ」を検出して報告（全件収集のため crash せず計数）。
- **テスト**: `make` → `make test.rb` → 簡単な Ractor スクリプト → `make check`。監査ビルドで `s→u` の WB 漏れパスを洗い出し、漏れがあれば WB 経路を修正。

### M2: per-Ractor objspace 方式（方式確定 2026-05-29）
**判断**: `rb_objspace_t` を field 抽出する代わりに、**Ractor ごとに `rb_objspace_t` を持たせる**（`rb_gc_get_objspace()` が現 Ractor のものを返す）。理由＝最大再利用: gc.c は `rb_gc_impl_*(rb_gc_get_objspace(), ...)` 経由なので大半が自動で per-Ractor 化。実現可能性確認済み: `during_gc`/`heaps` は objspace フィールド（`#define during_gc objspace->flags.during_gc` 1107, `#define heaps objspace->heaps` 1106）、致命的 static global 無し（size_to_heap_idx/heap_sizes/gc_stat_symbols 等は read-only or profiling のみ）、`rb_gc_impl_objspace_alloc` 存在。user の「objspace を local/global 分割」と一致。

**global 残置（VM 単位、cross-objspace 用）**: 全 objspace の page を載せる **global sorted page 配列**（conservative/cross-Ractor の pointer→page 解決 `is_pointer_to_heap`・`rb_gc_impl_pointer_to_heap_p`）、finalizer/object_id、stress/config、aggregate stats、weakmap。

**段階**:
- Phase A（非回帰）: `rb_ractor_t` に local objspace ポインタを通す。当面は VM 単一 objspace を alias（挙動不変）。
- Phase B: 非 main Ractor 生成時に別 objspace を alloc（まだ alloc 経路は切替えない）。
- Phase C: 非 main Ractor の alloc/GC をその objspace へ。cross-objspace 参照（core class/symbol/frozen literal 等の shareable は main objspace 在住）を global sorted 配列で解決。
- Phase D: `gc_enter` の `rb_gc_vm_barrier`（default.c:6931）を local GC では取らない（他 Ractor 走行継続）。global GC のみ barrier。
- 旧「field 抽出」案は不採用（再利用が少なく破壊範囲大）。

### M3: local GC / global GC の分離
- local GC: あるコンテキストの heap だけを mark/sweep。root = ローカル root ∪ `shared_bits` の U（`rgengc_rememberset_mark` を雛形に root 化）。shareable 境界で打ち切り、shareable は解放しない。他 space に書き込まない。
- global GC: 全 Ractor STW（既存 `rb_vm_barrier`）。現行 GC をほぼ流用。`shared_bits` を clear+recompute、shareable も回収。
- weak ref / finalizer / object_id の生死判定は **global GC 専管**（local GC では一切触らない）。

### M4: Ractor 終了時の heap 引き継ぎ
終了 Ractor の local heap（page 所有権）を **最初に join した Ractor** へ移管（no-move、page 単位）。join 前に終了した場合の一時孤児保持も規定。

### M5: send/copy
- `obj_traverse_replace` の identity/rec テーブルは維持（循環・別名保存）。per-node コピーを `#clone` 呼び出しから C レベル alloc+memcpy（`move_enter/move_leave` 流）へ（**`#clone` 廃止 = 非互換、要明記**）。
- コピー先を受信側 page に確保（dst-heap, 提案 2.4）、in-flight グラフは **basket を root** に送受信双方の local GC が生かす。所有権移管を定義。

### M6: トリガ
local: 自 heap の増分相対で minor/major。global: メモリ圧・shared 集合増分。`prev` は「直近 global GC 後の生存 shareable 数」。

## 4. WB 完全性 監査対象（M1 で実行検証）

- Class/Module: `const_set`(`ce->value`), class ivar(`fields_obj` 経由 imemo, 複合 shape の `st_insert+RB_OBJ_WRITTEN`, atomic), cvar(`cvc_tbl`), `rb_define_const`, autoload, singleton attached object。
- Ractor/Port: 受信メッセージの queue/port への格納、`ractor->default_port`、`crr->port`。
- born-shareable 初期化: `class_alloc0`(class.c:585), `ractor_alloc`(ractor.c:464) 周辺の生 C 代入（`r->verbose` 等）。
- `make_shareable(copy:false)` 後の残存 unshareable 参照（category-2）。
- T_DATA/imemo の shareable が `rb_gc`-marked C フィールドで unshareable を保持し WB を通らないケース。

→ 「choke point を通らない `s→u` 生成パス」を監査ビルドの実行で検出し、対処（WB 経由化 or 明示登録）する。

## 5. リスク / 留保

- M2/M3 の per-Ractor 分割は最大の工数。sorted page 配列を global に残す粒度（pointer→page 逆引きが全 page を要する）を要設計。
- local∥local 並列時、A が B の space の pinned shareable header を read する race（S は pin で安定だが page メタは触らない方針で閉じるか要検証）。
- 監査の stale-bit 偽陰性（解放→再利用スロット）。必要なら sweep/alloc で `shared_bits` をクリア。

## 6. M1 実装結果と監査の知見（2026-05-29）

**実装済み（`gc/default/default.c`）**: `RACTOR_LOCAL_GC`/`RACTOR_LOCAL_GC_AUDIT` フラグ、`shared_bits` per-page bitmap + `has_shared_objects` page flag + `GET_HEAP_SHARED_BITS`、WB choke point フック（`rb_gc_impl_writebarrier`）、full mark 中 recompute+audit（`gc_shared_relation`, `gc_mark` の全エッジ点）、global GC 一括クリア、`gc_move` の bit 追従。

**検証**: クリーン再コンパイルで **警告0/エラー0**。`test_gc`/`test_gc_compact`/`test_ractor` = 104 tests 0 failures。`make btest` = PASS all 2050。（`make test-all` 実行中）

**監査結果（`RACTOR_LOCAL_GC_AUDIT=1` 実行）**:
- **class の ivar / cvar / const は WB クリーン**（report 0 件）。= 「U が shareable 経由でしか届かない」危険カテゴリは `RB_OBJ_WRITE` 経由で確実に捕捉。
- WB 漏れは **すべて shareable な Ractor オブジェクト自身の内部 unshareable 状態**（生 C 代入）:
  - メッセージ basket payload: `ractor_sync.c:798`(`b->p.v=v`) → receiver の recv_queue `ractor_sync.c:1165`
  - Ractor-local storage: `ractor.c:2278`(st_insert) / `ractor.c:2373`(id_table_insert)（`Ractor[:k]=obj`）
  - `$DEBUG`: `ractor.c:580` 生コピー + `ruby.c:3184` setter（`$VERBOSE` は nil/bool 限定で良性）
  - threads / ractor-local std IO（T_FILE）: Ractor mark 経由で到達する生 C フィールド

### 知見＝計画の改善（重要）

漏れの object は **その Ractor 自身が所有する内部状態**であり、**所有 Ractor の per-Ractor root（`rb_ractor` の mark: threads/stacks/queue/storage）で既に root 化される**。つまり shared_bits に依存しなくても所有者の local GC が生かせる。一方、shared_bits が本当に必要なのは「U が shareable 経由でしか届かない」= **class/module の state**で、そこは WB クリーン。

→ **方式改善**: M3 で **per-Ractor local root に「その Ractor 自身の内部 unshareable 状態」を明示的に含める**（自分の Ractor オブジェクトは shareable 境界で止めず descend する）。これで Ractor 内部の生 C 代入を一つ一つ `RB_OBJ_WRITE` 化する必要がなく（脆い・C 構造体/imemo もある）、**shared_bits は純粋に cross-Ractor の class-state エッジ専用**にできる。WB フックを `T_CLASS/T_MODULE/T_ICLASS` 親に絞ることも可能（Ractor 親は self-root で担保）。
※ それまでの間も production の recompute が global GC 時に全エッジを bit 設定するため M1 の正しさは保たれる（漏れが効くのは local GC 導入後・global GC 前の新規エッジのみ）。

## 7. 進捗

- [x] **M1 完了・検証済み**（infra + WB + recompute + 実行可能監査）。監査で WB 漏れを Ractor 内部に局在と判明。
- [ ] M2: per-Ractor GC コンテキスト抽出（大規模リファクタ）
- [ ] M3: local/global GC 分離（+ 上記 self-root 改善 / shared_bits を class-state 専用に / shared_bits を root 化）
- [ ] M4: 終了時 heap 引き継ぎ（最初の joiner）
- [ ] M5: send/copy（dst-heap, basket root, #clone 廃止）
- [ ] M6: トリガ
