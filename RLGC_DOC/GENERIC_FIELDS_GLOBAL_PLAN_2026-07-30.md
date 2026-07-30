# generic_fields を Ractor-global に戻す計画 (2026-07-30)

ko1 判断: per-Ractor 表をやめ、global 一本に戻す。本書は現状の棚卸しと改善案。

## 現状

データ構造は実質 2 系統 + 所属遷移:

1. **per-Ractor 表** `r->generic_fields_tbl` (ractor_core.h:164) — unshareable な obj→fields imemo。
   owner thread 専有のため lock 無し。
2. **共有 global 表** — shareable の obj→fields。`vm->ractor.generic_fields_lock`
   (variable.c:71) で保護。
3. **所属の不変条件**: FL_SHAREABLE と表所属が 1:1 (`generic_fields_shared_p`,
   variable.c:1264-1269)。make_shareable の瞬間に `rb_mv_generic_ivar_to_shared` が
   per-Ractor → global へ entry を移送する。

付随機構:

4. **send-copy 同梱**: `r->gen_fields_capturing / gen_fields_capture / gen_fields_materialize`
   (ractor_core.h:168-170)。copy の走査で表対応を捕獲し basket に同梱、受信側 materialize が
   自分の表へ入れ直す。受信側が sender の per-Ractor 表を引かないための機構。
5. **死亡時移送**: `rb_ractor_absorb_generic_fields` (variable.c:2484、要素 merge は
   `gf_absorb_i`)。ractor_value では objspace merge より**前**に呼ぶ順序制約がある
   (merge 中の obj_free が rb_free_generic_ivar で joiner の表を引くため)。orphan merge
   (gc.c:4189) にも同経路。
6. **zombie 台帳の owner_slot** (gc.c:3955): global GC の weak pass が zombie の
   per-Ractor 表を舐めるために owner を記録している。
7. **weak/compaction pass は全表を回る**: `rb_generic_fields_tables_foreach`。移動 key の
   再挿入先を `gen_fields_current_tbl` (gc.c:4491-) で追跡。
8. 掃除は per-object 方式: obj_free → `rb_free_generic_ivar` (gc.c:2314-2318)。

規模: 関連コード ~195 行 (variable.c / ractor*.c / gc.c / default.c / ractor_core.h)。

## per-Ractor 化の当初の狙い (Stage1)

- owner アクセス (get/set/free) の lock-free 化
- 表所属 = 所有 Ractor で containment を素直に
- 死亡時に表ごと引き渡し (dst 空なら O(1))

## 何が問題か

- **「entry が今どの表にいるか」が状態遷移 (shareable 化 / send-copy / absorb / 死亡) と
  絡む**のが境界バグの温床。実績: genfields dangle ×2 (07-16) / drain poison UAF
  (8f4e30e93) / weak-pass O→Y WB miss (d0ffc359a) / compaction 共有表更新漏れ
  (8b61659ebf) / per-object 掃除回帰 (5b1176dabb) / T_HASH 二重複製 (60e4db56f4) /
  absorb 順序制約 / gf_absorb_i の WB 疑義 (未決)。
- 付随機構が広い: 同梱 3 フィールド + capture/materialize 経路、移送 2 経路、
  zombie owner_slot、複数表 foreach。
- upstream の fields_obj 常設化 (T_OBJECT/T_CLASS は inline、T_STRUCT/T_DATA は slot) で
  generic 表は **cold path 化**しており、lock-free の旨味が薄れた。

## 改善案: global 一本化

- **表**: 1 本の global st_table (既存の shared 表を全対象に拡張)。保護は既存
  `generic_fields_lock` のまま。
- **不変条件の消滅**: 表所属の遷移が無くなる (shareable 化は flag を立てるだけ)。
  fields imemo の objspace は従来通り setter (= obj の owner) の objspace なので
  containment は不変。unshareable への set は owner しか行えない (Ractor model) ため
  「他 Ractor の entry を書く」ことも起きない。
- **lock 規律** (最重要): 臨界区間は「確保なし・safepoint なし・park なし」の短区間に限定。
  fields imemo の確保や materialize の準備はロック外で行い、ロック内は
  insert/delete/lookup のみ。(根拠: TRY_WITH_GC deadlock 4adfe20009 と barrier-park の教訓)
- **GC 側アクセス**:
  - mark: 従来通り object 経由の表 lookup。上記規律により待ちは有界
    (holder は確保も park もしない)。single-Ractor mode (ever-multi 前) は lock 省略の
    fast path。
  - sweep: obj_free → rb_free_generic_ivar は同 lock で delete。
  - weak/compaction: barrier 下で 1 表を走査。`gen_fields_current_tbl` は不要化。
- **削除できるもの**:
  - `rb_ractor_absorb_generic_fields` + `gf_absorb_i` + ractor_value の順序制約
  - `gen_fields_capturing/capture/materialize` 一式 (受信 materialize は global 表へ直接 insert)
  - `rb_mv_generic_ivar_to_shared` の表移送 (flag 順序問題ごと消滅)
  - zombie 台帳の generic_fields 目的の owner_slot 依存 (weak pass が zombie 表を舐める
    必要が消える)
  - 複数表 foreach
- **残るもの**: fields imemo 自体の shareable 規律 (`rb_imemo_fields_record_shrefs` 等) は不変。

## 移行手順 (案)

1. global 表 API を一元化 (lookup/insert/delete を lock 規律付きで 1 箇所に)
2. mark / sweep / weak / compaction を 1 表前提に単純化
3. 書き手を global へ付け替え (`generic_fields_tbl_for` の分岐撤去)
4. 同梱・移送機構の削除
5. 検証: genfields 系 repro 群 (v2_shape_edges / l 系 / i_genivar_gc 等) + combined +
   CHECK/ASAN soak

## リスク / オープン

- local GC の mark が lock で短時間待つ = 純 lock-free からの一歩後退 (有界・cold path
  なので許容見込み。実測で確認)
- generic ivar が hot なワークロードの perf (upstream と同等に戻るだけ、とも言える。A/B 要)
- fork / free-at-exit の表再初期化パスの確認
- st_table は並行読み不可なので「mark も lock」を徹底。読みを lock-free にしたくなったら
  将来 concurrent map 化 (本計画の範囲外)

---

# 実装設計 (2026-07-30 追記)

## ロック API (variable.c に一元化)

```c
/* 全 generic_fields 表アクセスの唯一の入口。leaf lock: この下で他の lock を取らない。
 * may_alloc=true の窓は st_insert の rehash malloc が自 GC を起こして
 * mark→再 lock の自己 deadlock になるのを malloc_gc_disabled で防ぐ。 */
static inline bool
gf_lock(bool may_alloc)
{
    bool was = false;
    if (rb_multi_ractor_p()) {
        rb_native_mutex_lock(&GET_VM()->ractor.generic_fields_lock);
        if (may_alloc) {
            rb_ractor_t *cr = rb_current_ractor_raw(false);
            if (cr) { was = cr->malloc_gc_disabled; cr->malloc_gc_disabled = true; }
        }
    }
    return was;
}
static inline void gf_unlock(bool may_alloc, bool was) { /* 逆順 */ }
```

要点:
- **single-Ractor mode (`rb_multi_ractor_p()==false`) は無 lock**。GVL が mutator を直列化し
  GC も同スレッドで走る。st は rehash の malloc 時点で旧構造が consistent なので
  同スレッド GC の lookup も安全 (st の既存性質、upstream と同じ依拠)。
- **malloc_gc_disabled は save/restore**(counter 代わり)。RACTOR_LOCK 下から呼ばれる
  経路 (receive materialize) で既に true のことがあるため assert でなく退避。
- **lock 順序**: RACTOR_LOCK → gf_lock は可。gf_lock 下で RACTOR_LOCK / VM lock /
  確保(may_alloc 窓の st 内部以外) / safepoint / park は禁止 (leaf 規律)。
- 待ちの有界性: holder は「短窓・park なし・(may_alloc 窓は) 自 GC 遅延」なので、
  GC 側 (mark/sweep) が lock を待っても有界。自己保持は不可能
  (非 alloc 窓は GC を起こせず、alloc 窓は自 GC を遅延している)。

## 呼び出し部位ごとの変換

| 部位 | 現状 | 変換後 |
|---|---|---|
| get (`rb_gen_fields_tbl_get` 系) | 表選択+per-Ractor 無 lock / shared lock | `gf_lock(false)` + 単一表 lookup |
| set/insert (`rb_obj_set_fields` 系) | 同上 | **fields imemo と対応値はロック外で構築** → `gf_lock(true)` + st_insert |
| delete (`rb_free_generic_ivar`, sweep 文脈) | 表選択+条件 lock | `gf_lock(false)` + st_delete (st_delete は malloc しない)。zombie sweep (GET_RACTOR()==NULL) でも動く (cr NULL なら disabled 退避なし) |
| dup/clone (`rb_copy_generic_ivar`) | 〃 | lookup → ロック外で複製 → insert |
| mark (`rb_mark_generic_ivar`, local GC) | per-Ractor は無 lock 引き / global GC は weak pass 依存 | `gf_lock(false)` + lookup。**global GC 分岐は不要化も検討** (単一表なら STW 下で per-object 引きも安全) — 当面は現行の weak pass 方式を単一表化して維持 |
| weak/compaction pass | 全表 foreach + `gen_fields_current_tbl` | barrier 下で単一表を素走査 (lock 不要、assert barrier) |
| capture/materialize (send-copy 同梱) | 専用 3 フィールド+経路 | **廃止**。受信 materialize は通常 set 経路 (ロック外構築→insert) |
| shareable 化 (`rb_mv_generic_ivar_to_shared`) | 表移送+flag 順序 | 移送削除。fields imemo の shareable 化+`rb_imemo_fields_record_shrefs` のみ残す |
| absorb (`rb_ractor_absorb_generic_fields`+`gf_absorb_i`) | 2 経路+順序制約 | **削除** |
| zombie owner_slot (gc.c:3955) | weak pass の zombie 表走査用 | genfields 依存を削除 (他用途が残るかは S4 で確認) |
| atfork / free-at-exit | lock 再初期化+per-Ractor 表処理 | lock 再初期化のみ (既存 variable.c:79)。全 entry 処理は単一表 walk |

## コミット分割

- **S1**: `gf_lock/gf_unlock` 導入、既存 `generic_fields_write_lock` を置換 (挙動不変)
- **S2**: 書き手切替 — `generic_fields_tbl_for` を常に global に。get/set/delete/dup/
  copy/free-at-exit/mark を新 API 経由に統一。capture/materialize を通常経路へ
- **S3**: 死んだ機構の削除 (absorb 2 経路、同梱 3 フィールド、複数表 foreach、
  per-Ractor 表 field + atfork/free 処理、owner_slot の genfields 依存) ~ -150 行
- **S4**: 検証 — genfields repro 群 (i_genivar_gc / l 系 / v2_shape_edges 等) +
  combined ×10 + CHECK/ASAN soak 一晩 + **manyractor (60k) で lock 競合 spot check**
  (競合が出たら将来 N-shard 化、本計画の範囲外)

## 落とし穴メモ

- st_insert の rehash malloc は「旧構造 consistent のまま確保→swap」なので、遅延できない
  他 Ractor の GC が同時に lookup しても壊れない…は**成り立たない**(他 Ractor の GC lookup も
  gf_lock を取るので、そもそも並行しない)。並行するのは single-mode の自スレッド GC のみで、
  そこは st の性質で安全。
- weak pass と lazy sweep の delete: weak pass は STW barrier 中 = mutator/他 GC 停止で排他 ✓。
- `rb_multi_ractor_p()` は worker 全滅後も true のまま (fork でのみ復帰) → 復帰後も lock を
  取り続けるが正しさに影響なし (軽微な perf のみ)。
