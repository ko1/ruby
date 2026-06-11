# RLGCv2 設計

status: DESIGN — これを元に origin/master から実装する
date: 2026-06-10

Ractor ごとに独立した GC(Ractor-local GC)の設計。

## 全体像

- Ractor ごとに独立した objspace(ヒープ + GC 状態)を持つ。**main Ractor も同じ**で、
  特別な「VM の objspace」は存在しない。
- VM が直接指す GC のデータは `rb_global_objspace` ただ一つ。その中身は実質
  **ページプール(全 Ractor 共通のページ供給源)だけ**である。
- GC は「自分の objspace だけを、他の Ractor を止めずに回収する local GC」と、
  「全 Ractor を止めて全 objspace を一括回収する global GC」の二本立て。
- Ractor をまたぐオブジェクトの受け渡しは「shareable は参照のまま、unshareable はコピー」。
  これに例外を作らないことが GC の単純さを支える(§4)。
- Ractor が終了したら、その objspace は **join した Ractor が、いなければ main が
  ページごと引き継ぐ**(§2.3)。回収の主体を失う objspace は存在しない。

基本となる約束は次の 2 つ。以後の設計はすべてここから導かれる。

1. **封じ込め**: unshareable なオブジェクトへの参照は、その所有 Ractor の objspace の中に
   しか存在しない。(例外は送信中のメッセージだけ。pin で保護する。§4)
2. **single writer**: ある objspace のヒープ(freelist・ページリスト・各種カウンタ)を
   触るのは、その所有 Ractor のスレッドだけ。Ractor 内のスレッドは Ractor ごとの GVL で
   直列なので、ヒープ操作にロックは一切不要になる。

封じ込めから「local GC は自分の objspace の中だけ見れば生死を決められる」が出て、
single writer から「割り当ても GC もロック不要」が出る。

## 決定事項

1. objspace を rb_global_objspace(VM が唯一指す)と rb_objspace(Ractor ごと、main 含む)に
   分割する。
2. コンパイル時マクロや機能を切る環境変数は作らない(常時有効の一本道)。
3. newobj cache 層(master の `rb_ractor_newobj_cache_t` 相当)は作らない。割り当て状態は
   objspace のヒープに直接置く(single writer なので不要、§3)。
4. 空きページは global のページプールに返し、確保もそこから。Ractor は空きページを抱えない。
5. 終了した Ractor の objspace は、join(`Ractor#value`)した Ractor が**その場で**併合して
   引き継ぐ。join されないまま Ractor オブジェクトが回収されたら main が引き継ぐ。
6. 「unshareable はコピーでしか Ractor を渡れない」に例外を作らない。master に 1 つだけある
   例外 —『コピー不可の T_DATA でも、参照先がすべて shareable なら、コピーせず**同じ
   オブジェクトをそのまま**受信側のコピー結果に埋め込む』(ractor.c obj_traverse_replace_i の
   T_DATA ケース。例外オブジェクトの backtrace が該当)— を廃止して送信エラーにする(§4.4)。
   `Ractor#value` は「併合してから返す」ので例外にならない(§4.3)。
7. GC.enable / disable / stress / config / measure_total_time / stat / count は
   呼んだ Ractor の objspace に対する操作・表示。
8. fork は「自分以外の Ractor を殺してから」と同じ意味にする(子プロセスで他 Ractor の
   objspace は引き継ぎ機構で main に併合される)。専用機構なし。
9. VM 終了も「全 Ractor を殺す」だけ。全部が main に併合され、従来どおり main が
   finalizer を流して終わる。専用機構なし。
10. 統計カウンタは global に持たない。GC 回数等は objspace ごとの profile で足りる。
11. コピーはユーザ可視の `#clone` / `#initialize_clone` を呼ばない。コア型(String / Array /
    Hash など)は C の専用深コピーで書き、それ以外は当面 Marshal dump/load(Marshal の既存
    フック仕様には従う)。どちらにも乗らない型は送信エラー。残りは後で考える(§4.2)。
12. `define_finalizer` できるのは対象オブジェクトの所有 objspace(= 生成した objspace)
    だけ。他の Ractor から設定しようとしたらエラー。登録・テーブル・実行のすべてが所有
    Ractor に閉じる(§2.1)。
13. **WB-unprotected な shareable は unshareable を参照しない**、を不変条件とする。
    これにより shareable → unshareable の store の親は常に WB-protected で、この store は
    必ず write barrier を通る(shref の完全性が既存の世代別 WB と同じ規律に帰着する、
    §2.1)。「WB-unprotected がそもそも shareable になれるか」自体は要調査 — なれたと
    しても、この不変条件が成り立つ限り shref は壊れない。実装では make_shareable /
    FROZEN_SHAREABLE 系の既存検査(参照先がすべて shareable であること)に加え、
    shareable への wb_unprotect を assert で禁じて担保する。
14. **local major と global GC の起動要件は独立**。major は毎回 global へ昇格しない。
    local major は自分の旧世代(unshareable)の増加で、global GC は **shareable の世界の
    増加・滞留**(と終了済み Ractor の堆積)で起動する(§2.2)。
15. ページに **shareable_bits** を追加し、「local GC が解放してはならないオブジェクト」
    (shareable + 共有され得る VM 内部 imemo)をビットマップ化する。confined GC は
    これを索引に shareable を **root としてマークして**生かす(§2.1 — sweep で除外する
    方式は、生かしたオブジェクトの世代が進まず世代不変条件と衝突するため不採用)。
    ページ上の RLGC 追加ビットはオブジェクトあたり shref と合わせて 2 bit(§1.4)。
16. ユーザ定義 T_DATA の `dmark` / `dfree` は、upstream で導入予定の
    [Feature #22067](https://bugs.ruby-lang.org/issues/22067) の宣言機構に従う:
    宣言済みの型のみ local GC に参加し、未宣言の型は global GC だけが mark / free する
    (local GC 中に任意の C 拡張コードが他 Ractor と並行に走ることを防ぐ)。
17. **shareable から参照されるものは、例外を除いて shareable にする**。cc / cme /
    callinfo / iseq などメソッド・キャッシュ系の VM 内部オブジェクトは born-shareable に
    し、shareable_bits は FL_SHAREABLE と 1:1 に保つ。shareable → unshareable を許す
    例外は明示的なリストで管理する(§2.1)。

---

## 1. データ構造

### 1.1 全体図

```
rb_vm_t
  .gc.global_objspace ────→ rb_global_objspace(1 per VM)
                               └ page_pool(空きページ本体 + mmap アリーナ + アリーナ索引)

rb_ractor_t.objspace ───→ rb_objspace(1 per Ractor、main も同じ)
                              ├ heaps[size pool]: 割り当て用 freelist / 使用中ページ /
                              │   生きているページのリスト
                              ├ 世代別 GC の状態(remembered set 等)・mark stack
                              ├ malloc カウンタ(この Ractor の GC トリガ)
                              ├ finalizer テーブル・deferred finalizer
                              ├ GC ノブ(stress / config / measure …)・profile / 統計
                              └ mark_func_data(ObjectSpace 系の mark リダイレクト)

heap_page.objspace ─────→ そのページを所有する rb_objspace
```

「全 objspace の一覧」を持つ専用のデータ構造は**作らない**。objspace は必ずどれかの
Ractor の `r->objspace` であり(終了済み・未引き継ぎの Ractor も、引き継ぎが済むまで
VM の Ractor 一覧に「終了済み」として残す — §2.3)、`vm->ractor.set` を歩けば全部辿れる。
全 objspace の列挙が必要になるのは全 Ractor が停止しているとき(global GC、ObjectSpace の
ダンプ系、VM 終了)だけなので、この walk は常に安全な文脈でしか起きない。main objspace も
`vm->ractor.main_ractor->objspace` で辿れるため、専用ポインタは持たない。

### 1.2 rb_global_objspace = ページプール

```c
typedef struct rb_global_objspace {
    struct {
        rb_nativethread_lock_t lock;     /* ページ単位の操作のみ。保持は短い */
        struct heap_page_body *freelist; /* 返却された空きページ本体 */
        /* mmap した大きなアリーナ(2MiB アライン)群。ここからページ本体を切り出す。
         * アリーナの索引(ソート済み・追加のみ)が「このアドレスはヒープか?」の判定も担う */
        struct page_arena *arenas;
        char *arena_cursor, *arena_end;
    } page_pool;
} rb_global_objspace_t;
```

これだけである。次のものは global に**持たない**:

- 全 objspace の一覧 — VM の Ractor 一覧から辿る(§1.1)
- main objspace へのポインタ — `vm->ractor.main_ractor->objspace` で辿る
- 「Ractor が複数か」のフラグ — VM の multi-Ractor 判定(one-way)をそのまま使う
- 「global GC 中」のフラグ — バリア内で各 objspace に印を付ける(§2.2)
- ヒープのアドレス範囲 — ページプールのアリーナ索引が兼ねる
- 統計カウンタ — objspace ごとの profile で足りる(決定 10)
- チューニングパラメータ(RUBY_GC_* env)— master と同じ file static のまま

ページプールの役割:

- **確保**: freelist から pop → 無ければ現アリーナから切り出し → 無ければ新アリーナを mmap。
- **返却**: sweep で完全に空になったページは即ここへ返す(所有 Ractor は抱え込まない)。
  あるワークロードで膨らんだ Ractor のメモリを、他の・将来の Ractor が再利用できる。
- **アドレス判定**: アリーナは連続した mmap なので、「アリーナ範囲内のアドレスなら、64KiB
  アラインのページヘッダを安全に読める」が成立する。保守的スキャンは「アリーナ索引に
  当たる → ページヘッダの `objspace` を読む → 自分のものなら候補」と一直線になる。
  プール内のページと未切り出し領域はヘッダの objspace を NULL にしておく。
- ロックは leaf(保持中に割り当ても GC もしない)。操作はページ単位なのでオブジェクト
  割り当ての数千分の一以下の頻度であり、競合しない。注意点はページごとに mmap / munmap
  しないこと — それはカーネルの process-wide mmap_lock を直列化点にしてしまう。mmap は
  アリーナ単位(稀)に集約し、ページの再利用はユーザ空間の freelist で行う。

### 1.3 rb_objspace(Ractor ごと)

中身は master の objspace とほぼ同じ。違いは:

- `r->objspace` として全 Ractor が 1 個ずつ持つ(main も)。生成は Ractor 生成時。
- 割り当て状態(freelist・使用中ページ)を size pool ごとのヒープ構造が直接持つ
  (per-Ractor の newobj cache は存在しない。single writer なので不要)。
- 空きページ・アリーナを持たない(ページプールへ)。
- GC ノブ(stress / config / measure …)は objspace ごと。新しい Ractor は生成時に
  親 Ractor の設定を引き継ぐ。
- finalizer テーブルは objspace ごとで、**finalizer の実行もその Ractor のスレッドだけ**で
  行う(他の Ractor 上で勝手に走ることはない)。
- malloc カウンタも objspace ごとで、その Ractor の GC トリガを駆動する。
- `mark_func_data`(`ObjectSpace.reachable_objects_from` 等が mark を横取りして参照を列挙
  するためのリダイレクト)は **VM 共有にせず Ractor ごと**に持つ。VM 共有にすると、他の
  Ractor の並行 local GC の mark がリダイレクトに吸われる窓ができてしまう(ゲートで塞ぐ
  羽目になる)。Ractor ごとなら他人の GC を乗っ取る余地が構造的に無い(自分自身の GC との
  重なりだけ、従来どおり during_gc で無視する)。

### 1.4 ページとビットマップ

`heap_page` は master のもの + 次の拡張:

- **`page->objspace`**: 各ページのヘッダに、そのページを所有する objspace へのポインタを
  置く。オブジェクトのアドレスを 64KiB 境界に丸めるとページヘッダが得られるので、任意の
  オブジェクトについて「誰のものか」が 1 ロードで分かる(mark 中の自他判定がこれ)。
  `page->ractor` にしない理由: ページは GC の構造なので gc/ 層を VM の rb_ractor_t に
  依存させない、引き継ぎ待ち(所有 Ractor 不在)の期間や将来の「どの Ractor にも属さない
  共有ヒープ」でも指す先が常に実在する、の 2 点。Ractor が必要なら `objspace->owner` で
  1 段辿る。
- **shref_bits**: **shref**(= **sh**areable-**ref**erenced。shareable から参照されている
  unshareable、§2.1 で定義)に立てるページ上のビットマップ。
- **shareable_bits**: shareable に立てるページ上のビットマップ(意味は「**local GC が
  解放してはならない**」)。FL_SHAREABLE と 1:1 — cc / cme / callinfo 等の VM 内部
  オブジェクトも born-shareable にする(決定 17)ので、特別扱いの対象は無い。
  born-shareable な割り当てと `RB_OBJ_SET_SHAREABLE`(make_shareable)が header の
  フラグと同時に立てる。書き手は常に所有 Ractor のスレッド(封じ込めにより、どちらの
  操作も所有 Ractor 上でしか起きない)なので atomic は不要。bit を消すのは global sweep
  (`shareable_bits &= mark_bits`)と slot の解放時だけ。confined GC はこのビットマップを
  索引に shareable を root としてマークする(§2.1)。
- 複数スレッドが書き得るビットマップ(remembered set / shref_bits)への set は atomic CAS、
  ページ単位のフラグはビットフィールドでなく byte にする。非 atomic の `bits[i] |= mask`
  は並行する set を片方消し、関係ないオブジェクトの bit を落とす(young な子が解放される)。

### 1.5 Ractor 生成と VM インフラの置き場

割り当ては常に「実行中のスレッドの Ractor の objspace」へ入る(§3)。したがって Ractor の
生成で親スレッドが「子のための VM インフラ」を作ると、それは**親の objspace** に入る。
子専用の可変構造が親の objspace に居ると、子の local GC からは foreign(辿らない)、
親の local GC からは root が無い(親の物ではないから)、という宙ぶらりんになり、
use-after-free の温床になる。規則:

- 子 Ractor 専用の VM インフラは**子の objspace に確保する**。手段は 2 つ:
  1. **親が子の objspace へ直接確保する**(推奨)。子のスレッドが起動するまで子の
     objspace の writer は親だけなので、生成時に一時的に割り当て先を子へ切り替えるのは
     single-writer を破らない(stress GC が走っても、封じ込めガードにより空ヒープへの
     誤 root GC は no-op)。Thread / root Fiber の wrapper はこの方法で生成時から
     子の物にする — **オブジェクトの同一性が生涯変わらない**ことが重要
     (途中で作り直すと、起動初期に C レベルで掴まれた旧 wrapper と以後の wrapper が
     別オブジェクトになり、thread instrumentation のような identity ベースの API が壊れる)。
  2. 親の objspace に作られた物を**子の起動時に子側で作り直す**(割り込みキュー・
     mask スタックなど、identity が外部に出ない物はこちらで足りる)。
- Ractor 関連のインフラを新設するときは必ず「これはどの objspace に入り、誰の root から
  辿られるか」を確認する。レビュー観点として固定する。

## 2. mark & sweep 戦略

### 2.1 local GC — 自分だけを、止めずに回収する

各 Ractor は自分の objspace に対して minor / major GC を行う。**VM ロックもバリアも取らず、
他の Ractor の実行とも他の Ractor の local GC とも並行に走る。**

**root**: その Ractor が実行のために持っている参照の一式。具体的には各スレッド・fiber の
VM スタックとマシンスタック(保守的)、Ractor ローカル変数、この objspace の finalizer
テーブル。minor GC ではこれに remembered set が加わる。さらに RLGC 特有の root として
**shref(shref_bits の立ったオブジェクト)**が加わる(後述)。
他の Ractor のスタックは歩かない(並行実行中で不安定だから)。

**mark**: 自分の objspace の中だけを辿る。他の objspace のオブジェクトに行き当たったら
「生きている葉」として扱い、**辿らずに止まる**。相手の生死は相手の所有者(あるいは
global GC)が決める。自分の objspace に居る shareable は普通に辿る — その子(自分の
unshareable)を生かすのは自分の責務だから。ユーザ定義 T_DATA の `dmark` は決定 16 に
従う(宣言済みの型のみ local で mark し、未宣言の型は global GC に委ねる)。

**shref と shref_bits**: shareable から参照されている unshareable を **shref**
(**sh**areable-**ref**erenced)と呼ぶ — shareable の世界から、ある Ractor の私有グラフへ
参照が踏み込んでくる入口である(`Ractor.make_shareable` した構造の直下、送信中メッセージの
中身など)。封じ込めの例外的な向きはここに集中する: 親の shareable s は**他の objspace に
居るかもしれない**ので、shref u の所有者の local GC は s を辿らず(foreign だから)、
u への参照を**見つけられない**。そこで「`s.f = u` という代入が起きた瞬間に、u のページの
shref_bits に印を付け、所有者の local GC は shref を root 扱いする」ことで u を生かす。
これが維持できる理由も封じ込めにある: `s.f = u` と書けるのは u への参照を持つスレッド、
つまり **u の所有 Ractor 自身**だけ。だから write barrier は「自分のページに印を付ける」
だけでよく、他の Ractor のビットマップに書きに行く必要がない。
shref_bits は global GC の full mark のたびに全消去して付け直す(write barrier が維持し、
global GC が掃除する)。印の付け漏れ = 即誤回収、なのでここが封じ込めモデルの急所だが、
**s→u store の親になる shareable は常に WB-protected**(決定 13: WB-unprotected な
shareable は unshareable を参照しない)なので、「WB を通らない s→u store」は存在しない。
つまり shref の完全性が要求する規律は、既存の世代別 WB が要求するもの(バルクコピーの
後は remember を打つ、等)と同一であり、shref のための新しい監査項目は増えない。

**s→u が生じる場所(例外リスト)**: 原則として shareable から参照されるものは shareable に
する(決定 17)ので、s→u の例外は次に限られる。それぞれ生存機構が違う:

- **Class / Module のインスタンス変数・定数**に入る unshareable 値。これらは main Ractor
  からしかアクセスできない(既存の Ractor 仕様)ので「main の unshareable」であり、書くのも
  main 自身 → **shref**(WB が main のページに立てる。上の規律どおり)。
- **送信中メッセージ**(§4.2): 受信側のキューから、送信側 objspace の snapshot への参照。
  → **shref**(送信時に送信側が自分のページに立てる)。
- **Ractor オブジェクト**(shareable)は、その Ractor 専属の unshareable(`Ractor#[]` の
  storage、stdin / stdout / stderr など)を参照する。これは shref では扱わず、所有 Ractor の
  **root** にする(手順 3.c: rb_ractor_t から直接辿る。Ractor オブジェクト自体は生成元の
  objspace に居て、所有者から見ると foreign なので、オブジェクト経由ではなく C 構造体から
  root を引くのが正しい)。
- 他にもあり得る(候補: iseq が持つ実行時の可変スロット — once キャッシュ、coverage 等)。
  実装時に「shareable の mark 関数が辿る先」を監査し、見つけたものはこのリストに追加して
  「shareable にする / shref で守る(WB で書かれる物)/ root で守る(所有者の構造から
  辿れる物)」のどれかに割り当てる。

**sweep**: lazy でよい。ただし:

- **shareable は解放しない。** 他の objspace から参照されているかどうかを local GC は
  判定できないから。shareable の回収は global GC だけが行う。
  (クラスはもともと shareable。メソッドエントリ・コールキャッシュ等の VM 内部
  オブジェクトも born-shareable にする(決定 17)ので、同じ扱いに自然に含まれる。)
  実現方法は「**mark フェーズで shareable_bits を索引に root としてマークする**」
  (shref と同じ walk)。sweep 側でビット演算により除外する方式は採らない —
  生かしたオブジェクトがマークされないと age が進まず、「old の親 → 永遠に young の子」
  という remember されない O→Y エッジが生じて世代不変条件
  (GC.verify_internal_consistency)と衝突する。mark で生かせば普通に老化・昇格し、
  その子も traversal で自然に生きる。滞留計数(§2.2)は「この root 化で**新たに**
  マークされた数」(= 自分の root からは届かなかった shareable の数)として同じ walk で
  得られる。
  例外として、Ractor が 1 個しか居なければ local GC = 全体 GC なのでこの root 化ごと
  スキップし、shareable も普通に死ぬ。引き継ぎ(§2.3)で objspace は main 1 個に戻り
  得るので、この最適化は復帰可能にしておく。
- 完全に空になったページは**即ページプールへ返す**(直近 1 ページの保持などの
  ヒステリシスは実装の裁量)。
- finalizer 付きオブジェクトの zombie 化と deferred finalizer の実行は、この objspace の
  所有 Ractor のスレッドだけで行う。**登録も所有 objspace からのみ**: 他の Ractor の
  オブジェクト(shareable 含む)に `define_finalizer` しようとしたらエラー(決定 12)。
  これで finalizer は登録から実行まで完全に Ractor 内に閉じ、cross-objspace の置き場
  問題が消える。

#### 手順: local GC

minor / major とも自スレッドで実行し、ロックもバリアも取らない。master の GC との差分に
★を付ける。
(実装は 2 段階: M1a では従来どおり VM lock + barrier の下で動かして封じ込めの正しさを
固め、「ロックもバリアも取らない」は M1b で達成する — §5 の順序と理由を参照。)

0. 前提: 自分の lazy sweep が残っていれば先に完走させる(master と同じ)。`during_gc` を
   立てて再入を防ぐ。incremental marking は objspace が複数ある間は使わない★
   (単一 Ractor のときだけ master のまま。2 個目の Ractor を作る時点で、進行中の
   incremental marking は完走させてから移行する)。
1. minor / major の選択は master と同じ基準(自分の旧世代の増加・malloc 量・明示指定)。
   ★major は local のまま実行する — 目的は自分の unshareable の旧世代の回収であり、
   STW は要らない。global GC の起動は別の基準(shareable の増加・滞留、§2.2)で判断し、
   その条件を満たしているときだけ local major の代わりに global GC を要求する。
2. mark の準備:
   - minor: master と同じ。旧世代(昇格済み + remembered な wb-unprotected)は生存前提
     から始め、mark bit はクリアしない。
   - major: 自分の全ページの mark / marking / uncollectible / remembered bit をクリア
     (master の major と同じ)。★ただし shref_bits はクリアしない — 「外の shareable が
     自分の誰を参照しているか」は自分からは列挙できないため、shref の再計算は global GC に
     しかできない。local major は WB が維持してきた値をそのまま信じる。
3. root を mark する。すべて「自分のもの」だけ★:
   a. 自 Ractor の各スレッド・各 fiber の VM スタック / EC。suspended な root fiber も
      wrapper 経由でなく直接辿る★(wrapper オブジェクトは他 objspace に居ることがある)。
   b. 自スレッドのマシンスタック・レジスタ(保守的)。word ごとに「ページプールの
      アリーナ範囲内か → ページヘッダの objspace == 自分か → 有効な slot 先頭か」で
      ★**自分の**オブジェクトだけを候補にし、mark + pin する。foreign を指す word は
      無視する(その生存は所有者か global GC の責任)。
   c. Ractor self と Ractor-local storage。
   d. 自 objspace の finalizer テーブル(値を pin)。
   e. VM-global の registered roots★(`rb_global_variable` / `rb_gc_register_mark_object`)。
      登録リストは VM に 1 つのまま、**全 objspace の root 走査が C レベルで全エントリを
      なめる** — mark 側の foreign-skip が「自分の objspace のエントリ」だけを自然に
      選別する(リストのチャンク自体も登録した Ractor の objspace 生まれなので、所有者が
      mark して生かす)。こうしないと「VM グローバルな表に入れた worker のオブジェクトを
      誰も root にしない」という穴が開く(lazy 初期化の static 変数を worker が先に踏む
      ケースで実証済み: `clone(freeze: true)` の freeze_true_hash 等)。
      per-objspace の登録表に分割する案は不採用 — `rb_global_variable(VALUE *)` は
      アドレス登録で、スロットには後から**別の objspace の値**が代入され得るため、
      表の所有 objspace を決められない。走査コストは O(全登録数) × objspace 数だが、
      登録物は定数規模(チューニングは M5)。
   f. ★shareable と shref を root 化: `has_shareable_objects` / `has_shref_objects` の
      立った自分のページを走査し、shareable_bits | shref_bits の立ったオブジェクトを
      root として mark する(bit の在処は自分のページなので走査は自己完結)。
      このとき「新たにマークされた shareable の数」を数えておく — 滞留推定(§2.2)。
   g. minor のみ: remembered set(master と同じ。remembered ページの旧世代の子を再走査し、
      wb-unprotected な uncollectible も再走査する)。
4. 推移的 mark(mark stack が空になるまで):
   - 子 c を辿る前に★ `GET_HEAP_PAGE(c)->objspace` を見る。自分でなければ**何もしない**
     (bit も立てず、子も辿らない)。これが封じ込めの実行点。
   - 自分のものなら master と同じ: mark bit を立て、age を進めて昇格を判定し、子を積む。
     自分の shareable も普通に辿る。weak 参照は後処理用に積む。
   - bitmap の書き込み規律★: mark / marking / uncollectible 系を触るのは自スレッドだけ
     (plain store 可)。remembered / shref は他 Ractor の WB が並行に書くので atomic。
5. mark の終了処理: weak 参照のうち対象が**自分の** unmarked のものをクリアする。
   ★対象が foreign のものは触らない(global GC が処理する)。世代カウンタの更新は
   master と同じ。
6. sweep(lazy 可。master の枠組みに以下の差分):
   - shareable は手順 3.f の root 化により必ずマーク済みなので、sweep 自体は master の
     「unmarked を解放する」のままでよい(★sweep に shareable の特別扱いは無い)。
   - finalizer 持ちは zombie 化して自分の deferred リストへ(実行も自スレッド)。
   - ★解放した slot の shareable / shref ビットはクリアする(再利用に引き継がない)。
   - ★完全に空になったページは `page->objspace = NULL` にして global page pool へ返す。
7. 終了: `during_gc` を下ろし、deferred finalizer を通常の機構で実行する。

#### 手順: write barrier(GC の外で動く維持機構)

`RB_OBJ_WRITE(a, &slot, b)` のとき:

1. 世代: a が旧世代で b が新世代なら a を remember する(master と同じ)。ただし★
   shareable への store は他 Ractor のスレッドからも来るので、remembered_bits と
   ページフラグは atomic に立てる。
2. ★shref: a が shareable で b が unshareable なら、b のページの shref_bits を立てる。
   封じ込めにより、この store を実行できるのは b の所有 Ractor のスレッドだけなので、
   これは常に「自分のページへの書き込み」で済む。

前提(決定 13): s→u エッジの親になる shareable は必ず WB-protected — つまり
WB-unprotected な shareable は unshareable を参照しない(FROZEN_SHAREABLE 系の T_DATA は
make_shareable 時に「参照先がすべて shareable」を検査済みのうえ frozen であり、
shareable への wb_unprotect は assert で禁じる)。よって上の 2 経路が WB で漏れなく
踏まれることは、既存の世代別 WB と同じ規律で保証される。

### 2.2 global GC — 全部を止めて、全部を回収する

VM バリアで全 Ractor を停止し、全 objspace を一括で mark & sweep する。shareable・
VM 内部オブジェクト・「自分からは辿れないが他の objspace からは生きている」ものを
到達性どおりに回収できる唯一の機会である。

**起動要件は local GC とは独立**。local major が「自分の旧世代(unshareable)が育った」
ことを見るのに対し、global GC は「**shareable の世界が育った**」ことを見る — shareable は
local GC では回収できず、global GC まで滞留し続けるからである。起動条件(いずれかを
満たすと、気づいた Ractor が driver になって要求する):

1. **shareable の増加**: 自分の objspace の shareable 数 `shareable_objects` が
   `shareable_objects_limit` を超えた。旧世代の `old_objects > old_objects_limit` と
   同型のルールである。計数は born-shareable な割り当てと `RB_OBJ_SET_SHAREABLE` での
   increment(どちらも所有スレッド上、plain でよい)+ 引き継ぎ併合(§2.3)での加算。
   local GC は shareable を解放しないので、この計数は global GC 間で正確な生存数を保つ。
   limit は global sweep が shareable_bits の popcount で正確な生存数を取り直し、
   `生存数 × factor(既定 2.0 — 旧世代の GC_HEAP_OLDOBJECT_LIMIT_FACTOR と同じ)+ 下限`
   で再設定する(下限が無いと新しい Ractor が 1 → 2 で即発火してしまう)。
2. **滞留の観測**: 自分の local GC が数えている「shareable の root 化(§2.1 手順 3.f)で
   **新たに**マークされた数」 — 自分の root からは届かない shareable、すなわち自分の
   ヒープに滞留している「local では回収できないゴミ」の上界推定(cc / cme 等の VM 内部
   オブジェクトも含む)— の比率が閾値を超えた。
3. **終了済み・未 join の Ractor** の objspace が溜まった(回収・併合できるのは
   global GC だけ、§2.3)。
4. 明示(`GC.start`)と VM 終了。

判定に使う計数はすべて**自分の objspace のもの**なので、global な状態も他 objspace の
読み取りも要らない。条件 1 は per-Ractor 判定のため「全 Ractor が一様に 2 倍未満ずつ
育つ」ケースを取りこぼすが、それは条件 2(滞留比率)が拾う。係数・下限の既定値は
実装しながら調整する(M5)。global GC は STW を伴うので頻度が性能に直結する(毎 major
昇格にした場合の実測例: N=8 で STW コスト ~13%)— 起動条件をこのように分離するのが
その対策である。

- barrier 内で driver はまず全 objspace に「global GC 中」の印を付ける(全員止まっている
  ので安全)。mark / sweep の各判定はこの per-objspace の印を見る。global 専用の状態は
  持たない。barrier を出る前に全部下ろす。
- mark は封じ込めを解除して全 objspace を 1 本の mark stack で辿る。shref_bits は全消去
  してこの full mark 中に付け直す。
- **root は全 objspace 分を漏れなく**。「sweep は全空間一括なのに root mark が一部の
  objspace の分しか辿られない」という非対称は、そのまま use-after-free になる(この設計で
  最も事故りやすい点)。そこで「objspace の root 一覧」を 1 箇所に表として持ち、local GC と
  global GC が**同じ表**から root を引く実装にする(片方にだけ root を足して漏らす事故を
  構造的に防ぐ)。
- sweep も barrier 内で全 objspace に対して行う。空きページはページプールへ。

なお STW 中は並行する書き手が居ないので、driver が他 objspace のビットマップへ書く
(pin の付け直し等)のは安全である。local GC 中は不可。この区別のための述語を 1 つ用意する
(「いま local GC か?」)。

#### 手順: global GC

1. 契機: 上記の起動条件(shareable の増加・滞留・zombie Ractor の堆積・明示)を満たした
   Ractor が driver になる。Ractor が 1 個のときは local major で足りるので起動しない。
2. VM バリアを取る。進行中の local GC は完走を待つ(local GC は途中でバリアに合流
   しない)。バリア成立後は、mutator も他の GC も一切動いていない。
3. 全 objspace の残っている lazy sweep を driver が完走させる(STW 中なので他人の
   objspace を触って安全。mark bit の意味を次のクリアの前に確定させるため)。
4. 全 objspace に「global GC 中」の印を付ける。
5. クリア — **全 objspace の全ページ**について、mark / marking / uncollectible /
   remembered / shref_bits をクリアし、世代カウンタをリセットする(major のクリア +
   shref)。全列挙は VM の Ractor 一覧で行う(終了済み・未引き継ぎの Ractor を含む)。
   ※ ここで objspace を 1 つでも取りこぼすと、stale な mark bit の残ったオブジェクトが
   「既マーク」扱いになって子が辿られず、使用中の表(メソッドキャッシュ等)が sweep されて
   UAF に至る — 最悪の壊れ方をするので、全列挙の完全性はこの設計の生命線。
6. root を mark — root 表(local GC と共通の一覧)を**全 Ractor / 全 objspace 分**処理する:
   - VM グローバル root(vm 本体、グローバル変数、trap、…)
   - 全 Ractor のスレッド / fiber のスタック(マシンスタックの保守的 scan は、word の
     所有 objspace をアリーナ索引 → ページヘッダで特定し、**その objspace の**ページに
     mark + pin する)
   - 全 objspace の finalizer テーブルと registered roots
   - 全 Ractor のキュー / port 上の送信中メッセージ: クリア(5)で pin が消えているので、
     shref を立て直して mark する。受信側で実体化中のメッセージも同様。
   - suspended な root fiber
7. 統一 mark: mark stack は 1 本、封じ込めは解除。子がどの objspace に居ても、その
   ページに bit を立てて辿る(STW なので他空間のビットマップへの書き込みも安全)。
   mark 中に shareable → unshareable のエッジを踏んだら、その場で子の shref_bits を
   立て直す — これが shref の再計算で、以後は WB が次の global GC まで維持する。
8. weak 参照を全空間分処理する。
9. global sweep — バリア内で完結させ、lazy にしない。全 objspace の全ページについて:
   - ここでは shareable も解放する(統一 mark の到達性は正確): 解放候補 =
     `有効 slot & ~mark_bits`(shareable_bits は見ない。cc / cme 等も shareable として
     ここで回収される)。ページごとに `shareable_bits &= mark_bits` と畳んで生存分だけに
     更新する。
   - zombie は各 objspace の deferred リストへ(実行はその所有 Ractor のスレッド。
     終了済み objspace の分は引き継いだ側が実行する)。
   - 空ページはページプールへ返す。
   - 後始末: この cycle で Ractor オブジェクトが回収された「終了済み・未 join」の
     objspace を main に併合する(§2.3)。
10. 全 objspace の「global GC 中」の印を下ろし、バリアを解除する。

### 2.3 Ractor 終了 — objspace は join した者が、いなければ main が引き継ぐ

終了した Ractor の objspace は、その場では誰にも併合されず、終了済みの Ractor に
ぶら下がったまま残る(ブロックの戻り値もそこに入っている)。専用の管理リストは作らない。
引き継ぎが実行されるのは次のどちらかの時点で、**どちらも「引き継ぐ側が安全に自分の
ヒープを触れる文脈」に最初からある**:

- **join された場合**: `Ractor#value` を呼んだ Ractor R は、相手の終了を待ったあと、
  **R 自身のスレッドの中で**死んだ objspace を自分のヒープに併合し、それから戻り値を返す
  (§4.3)。併合は自分のヒープへの書き込みなので single writer はそのまま守られる。
  value の呼び出し自体が引き継ぎの実行場所であり、受け渡しの仕掛けは何も要らない。
- **join されないまま Ractor オブジェクトが回収された場合**: Ractor オブジェクトは
  shareable なので、回収するのは必ず global GC(STW 中)。全員止まっているので、
  global GC がバリアの中で(sweep の後始末として)その objspace を main に併合する。
  STW 中は single writer の制約自体が発生しない。

併合の作業内容はどちらも同じ: ページを size pool ごとに引き継ぎ側のヒープへ繋ぎ替え、
各ページの `page->objspace` を書き換え、finalizer テーブル・zombie・カウンタ類を併合し、
空きページはページプールへ返し、objspace の殻を解放する。死んだ Ractor の deferred
finalizer は以後**引き継いだ側のスレッド**が実行する(終了した Ractor にはそれを実行する
スレッドが無い — 放置すると zombie が永遠に残り、objspace は決して空にならない。
引き継ぎがその答えになっている)。

終了から引き継ぎまでの間:

- この objspace を local GC する者は居ない(所有者不在)。中のゴミは global GC が回収し
  続ける。戻り値とそこから辿れるものは、終了済み Ractor 経由で生きている。
- 「全 objspace は Ractor の一覧から辿れる」を保つため、終了済み・未引き継ぎの Ractor は
  VM の Ractor 一覧に**終了済みとして残し**、引き継ぎ完了で外す。プロセスで言う zombie と
  同じ構図(join = wait、main = init への reparent)。新しいリストではなく既存の一覧の
  延命である。ただし**バリアの参加者としては数えない**(スレッドが無いので合流できない)し、
  「Ractor が 1 個か」の判定(単一 Ractor 最適化、§2.1)でも生きている Ractor だけを数える。
- 封じ込めにより、この objspace の unshareable に外から刺さる参照は無い(戻り値は併合後に
  しか Ruby コードへ返らない、§4.3)。外から参照され得るのは shareable だけで、それは
  誰の local GC も解放しない。だからこの待機状態は安全。

fork(決定 8)は「子プロセスで他の全 Ractor をこの『未 join 終了』扱いにする」、VM 終了
(決定 9)は「全 Ractor を終了させて main が引き継ぐ」で、どちらも専用機構なしにこの上に
乗る。Ractor が main 1 個に戻れば local GC = 全体 GC なので shareable も回収できる。

### 2.4 VM 共有テーブルと local GC

local GC はロックを取らずに走るため、「local GC のコードパスが読み書きする VM 共有の
可変構造」は、一つずつ扱いを決めておく必要がある(漏れ = 並行クラッシュ)。原則は 3 分類:

1. **shareable しか載らない表は、同期不要**。fstring(frozen string interning)表や
   dynamic symbol 表のエントリは shareable であり、local GC は shareable を解放しない
   (shareable_bits)。つまり**これらの表からの削除は global GC(STW)中にしか起きない**
   ので、mutator 側の挿入(既存の同期のまま)と local GC が競合する経路が存在しない。

   注意: エントリだけでなく**表の入れ物(コンテナオブジェクト)自体も shareable で
   なければならない**。concurrent set(fstring 表・sym 表の実体)は resize 時に
   「resize したスレッド」の objspace に新世代の T_DATA を確保して C グローバルを
   差し替える。worker の objspace に生まれた表は worker の local root から届かず、
   main から見れば foreign なので、shareable 化しないと worker の local GC が
   生きた表ごと回収してしまう(実際に M1a で fstring 表がこの経路で壊れた)。
   同型: symbol の id→(str,sym) 逆引きに使う `id_entry_list`(T_DATA)も intern した
   Ractor の objspace に生まれ、main 在住の `symbols->ids` Array からしか参照されない。
   どちらも born-shareable にして所有 objspace の pin で守る。代償として、resize で
   不要になった旧世代の表は global GC まで回収されない(retention)が、旧世代の合計は
   最終サイズの定数倍で抑えられるので許容する。
   一般則: **「VM グローバル(C global / VM 構造体)から届く GC オブジェクトを
   main 以外のスレッドが確保する」箇所は、必ず born-shareable にする**(決定 17 の系)。
   born-shareable にできない(意味的に unshareable な)ものは、§2.1 手順 3.e の
   VM-global 登録リスト(全 objspace が C 走査)に載せる — 例: `clone(freeze:)` の
   freeze_true/false_hash や `<cfunc>` 文字列のような lazy 初期化 static。
2. **オブジェクトに紐づく表は per-objspace に分割する**。generic ivar の表
   (非 T_OBJECT ホストの ivar 置き場)はホストごとのエントリなので、ホストの所有
   objspace の表に分ける。挿入・削除(local sweep での解放時)・mark 中の参照がすべて
   single writer に戻り、同期が消える。
3. **分割できない表は短い専用ロック**。`object_id` の対応表(id → obj)は「任意の id を
   引ける」ことが意味なので VM 全体で 1 つ。id を持つ unshareable の解放(local sweep)
   時の削除と、`_id2ref` / id 付与の挿入・参照を専用ロックで同期する(global GC バリアに
   合流しない種類のロックにすること — local GC の途中でバリアに巻き込まれてはならない。
   頻度は「id を持つオブジェクトの解放時」だけなので低い)。

実装では「local GC から触る VM 共有構造」を列挙し、必ずこの 3 分類のどれかに割り当てる。

補足: **VM 全体のヒープ走査**が要る操作のために `rb_objspace_each_objects_all`
(VM lock + barrier 必須、`vm->ractor.set` の全 objspace を順に走査)を用意した。
TracePoint の iseq 計装(`rb_iseq_trace_set_all`)・attr/bf コールキャッシュの一掃・
coverage 削除はこれを使う(per-objspace 走査のままだと「worker で TracePoint を
enable しても main の iseq が計装されない」)。なお Ruby レベルの
`ObjectSpace.each_object` は現状 per-objspace 走査のまま(multi-Ractor 時は従来どおり
shareable のみ yield)で、「全 objspace の shareable を見せる」master 互換にするかは
M2 で決める。注意: `cr->objspace` はこの走査の入力なので、一時的に差し替える処理
(Ractor 生成時の子 objspace への割り当て)は必ず VM lock 下で行い、barrier を張った
walker から差し替え中の状態が見えないようにする。

### 2.5 compaction

objspace が複数ある間は不可(オブジェクトを動かすと、他 objspace からの参照・shref_bits・
「shareable は動かない」前提が全部壊れる)。`GC.compact` / `GC.verify_compaction_references` /
`GC.auto_compact=` の **3 経路すべて**をガードする — どれか 1 つでも漏れると、worker の
居る状態の full GC が compaction を実行してヒープ全体が壊れる。Ractor が 1 個のときは
従来どおり許可。

## 3. newobj 戦略

割り当ては自分の objspace から、ロックなしで行う。これがこの設計の存在理由である
(master では cache miss のたびに VM 全体のロックを取るため、全 Ractor の割り当てが
直列化してスケールしない)。

```c
static VALUE
newobj(rb_objspace_t *os, size_t heap_idx)
{
    rb_heap_t *heap = &os->heaps[heap_idx];
    struct free_slot *p = heap->freelist;     /* 触るのは所有 Ractor だけ(single writer) */
    if (LIKELY(p != NULL)) {
        heap->freelist = p->next;
        return (VALUE)p;
    }
    return newobj_refill(os, heap);           /* ここもロックなし */
}
```

freelist が尽きたときの補充も全部「自分の物」で進む:

1. 自分の sweep 済みページ(空きスロットを持つ生きページ)に割り当て先を切り替える。
   lazy sweep 中なら自分の sweep を一歩進める。
2. 無ければ、成長してよいか判定して(チューニングパラメータと自分の状況)、
   **ページプールから 1 ページもらう**。
3. 成長させないなら、**自分の local GC を実行**(同じスレッドで。再入は objspace の
   `during_gc` で防ぐ)。major が必要で Ractor が複数いるなら global GC へ昇格 —
   バリアを取るのはこのときだけ。
4. それでも足りなければ memerror。

そのほか:

- malloc 量の計上も objspace ごとで、その Ractor の malloc トリガ GC を駆動する。
- GC.stress は自分の objspace のノブで、自分の local GC を起こすだけ。
- boot 最初期(main Ractor 生成前)は main の objspace に直接割り当てる。
- クラス・メソッドエントリ・コールキャッシュ等の VM 内部オブジェクトも、作った Ractor の
  objspace に置く(正しさのために main へ寄せる方式は採らない。性能のための共有ヒープは
  将来課題)。
- NEWOBJ / FREEOBJ の tracepoint: NEWOBJ は従来どおり(フック有効時のみ VM ロック下)。
  FREEOBJ のフックは worker の objspace には立てない(worker の local sweep 中に任意の
  Ruby / C コードが走ることを防ぐ。これが立たないことは安全性の前提)。

## 4. message send 戦略

### 4.1 三つの経路

| 渡すもの | 経路 | 生存の保証 |
|---|---|---|
| shareable | 参照のまま | local GC は shareable を解放しない + global GC が到達性で回収 |
| unshareable の send / yield | **コピー**(§4.2) | 送信中 pin + 受信側で実体化 |
| unshareable の `Ractor#value` | **参照のまま**(併合してから返す、§4.3) | 返る時点で受け手自身のオブジェクト |

`move:` はコピーと同じ流れで、送信側の元グラフを無効化する点だけが違う。

### 4.2 コピーは「受信側で実体化」

コピーを 1 回のトラバースで受信側の objspace に直接作ることは**できない**。
送信時に作るなら送信スレッドが受信側のロックなしヒープに書くことになり single writer が
壊れる。受信時に 1 回で作るなら send 後の変更がコピーに混ざり snapshot 意味論が壊れる。

よって 2 段階にする:

1. **send 時**(送信スレッド): 自分の objspace に snapshot コピーを作り、キューに積み、
   **送信中 pin** を付ける(shref_bits を立てる。キューに積まれている間、メッセージは
   「共有の世界から参照されている」= shref そのものなので、同じ仕組みで守れる)。
2. **receive 時**(受信スレッド): snapshot を自分の objspace へ実体化して受け取る。
   snapshot は送信側のゴミになる。

コピーの実装はユーザ可視の `#clone` / `#initialize_clone` を**呼ばない**(決定 11):

- String / Array / Hash などのコア型は、C で専用の深コピーを書く(上の 2 トラバース)。
- それ以外の型は当面 **Marshal**: send 時に `Marshal.dump`(snapshot がバイト列 =
  送信側の String 1 本になり、それが in-flight pin の対象)、receive 時に受信側で
  `Marshal.load`。load は通常の割り当て・WB 経路を通るので、下の世代の整合も自然に満たす。
- どちらにも乗らない型(`_dump` を持たない T_DATA 等)は送信エラー(§4.4 と同じ規則)。
  残りの細部は後で考える。

pin は global GC をまたいでも維持する(global GC は shref_bits を全消去するので、
キュー上のメッセージは global GC 中に付け直す。キューから外れて実体化中のものは
受信 Ractor 側に「実体化中スロット」を設けてそこから付け直す)。

**世代の整合**: 受信側での実体化は「old な親 → young な子」の参照を受信側 objspace の
中に作る。このとき remembered set に登録されないと、次の minor GC が子を解放して即
クラッシュになる(例: generic ivar に Array 値を持つ深いグラフの送信は、登録漏れがあると
決定的に再現する)。**実体化が作るすべての参照ストアは受信側の write barrier を通す**ことを
実装の要件とする(generic ivar の構築や Array / Hash の充填のような「生ストア」も含めて)。

### 4.3 `Ractor#value` は「併合してから返す」

`Ractor#value` は successor(最初に value を要求した唯一の Ractor。二人目はエラー)に
ブロックの戻り値を**コピーせず**返す。これが封じ込めと矛盾しない理由は順序にある:

```
相手の終了を待つ → 死んだ objspace を自分のヒープに併合する(§2.3)→ 戻り値を返す
```

value から返った時点で、戻り値はすでに successor 自身の objspace のオブジェクトであり、
「他人の unshareable への参照」が Ruby コードに渡る瞬間は存在しない。ゼロコピーのまま、
封じ込めの例外にもならない。例外終了の場合(value が raise する側)も同じで、併合してから
raise する。

誰も value を呼ばなければ、戻り値は終了済み Ractor の objspace に入ったまま global GC に
管理され(§2.3)、Ractor オブジェクトの回収と同時に main へ行く。その時点で value は
二度と呼べない(Ractor オブジェクトへの参照が無い)ので、取り損ねは起きない。

### 4.4 参照のすり抜けを許さない

master の Ractor コピーには「move 不可で、直接参照が全部 shareable な T_DATA は、コピーせず
同じポインタを埋め込む」という例外がある(ractor.c の obj_traverse_replace_i)。単一ヒープ
なら無害な最適化だが、per-Ractor objspace では「受信側のコピー済みグラフの中に、送信側
objspace の unshareable T_DATA への生ポインタが残る」ことを意味し、どの生存保証にも
引っかからず UAF になる(例: 例外オブジェクトの backtrace がまさにこの形に該当する)。

扱いは型ごとに 3 通り:

- **backtrace は専用のネイティブ複製**(`rb_backtrace_dup`)。フレームが参照するのは
  iseq / メソッドエントリ(決定 17 で shareable)だけなので、複製は封じ込めに反しない。
  文字列配列・Location 配列は受信側で lazy に再生成される。これにより例外の送信で
  `backtrace` / `backtrace_locations` の両方が保たれる(エラー化すると例外伝搬そのものが
  壊れるため、この型だけは複製で救う)。
- それ以外の unshareable T_DATA は **Marshal に乗れば乗せ、乗らなければ送信エラー**
  (`_dump` 系を持つ型は受信側で別オブジェクトとして実体化される)。
- shareable な T_DATA は従来どおり参照渡しで問題ない。

ネイティブコピーの enter は対応型以外で即 stop して Marshal 経路へ落ちるので、
**by-ref passthrough にはコピー経路から到達できない**(move 経路は従来どおり
「move できない T_DATA はエラー」)。今後も「コピーをサボって参照を渡す」最適化を
send 系に入れないこと。なお `Ractor#value` がコピー無しで済むのはすり抜けではなく、
参照を返す前に objspace ごと併合するからである(§4.3)。

## 5. 実装計画(origin/master から。各段で全テスト green)

順序は **M0 → M1a → M3 → M2 → M4 → M1b → M5**。local GC の並行化(M1b)を
最後尾近くまで遅らせるのが要点(理由は M1b の項)。並列性能が出るのは M1b 以降で、
それまでの各段は「master と同等性能・正しさは段ごとに full green」を保って進める。

- **M0 土台**: rb_global_objspace(ページプール)新設、`vm->gc.objspace` を
  `vm->gc.global_objspace` に差し替え、main objspace を main Ractor 持ちに、newobj cache を
  剥がしてヒープ直割り当てに(単一 Ractor なら自明に single writer)。master と性能比較。
- **M1a per-Ractor objspace と local GC(STW 段階)**: Ractor 生成で objspace 生成、
  封じ込め mark、shareable / shref の root 化 pin(§2.1 手順 3.f)、shref_bits と
  write barrier。**この段階では gc_enter が従来どおり VM lock + barrier を取る** —
  「自分のヒープしか刈らないが、刈る間は全 Ractor が停止する」。§2.1 冒頭の
  「ロックもバリアも取らない」はまだ実現せず、M1b で達成する。並行性バグが存在しない
  世界で、封じ込め・pin・root 集合の正しさだけを固めるための分割。
  (global GC がまだ無いので shareable は滞留する。M2 で解消。)
- **M3 message send**: 受信側実体化(コア型の C 深コピー + Marshal 経路)、送信中 pin
  (global GC またぎ含む)、**受信側 write barrier の徹底**(§4.2 世代の整合)、
  T_DATA passthrough の廃止、value の successor 規則(§4.3。併合本体は M4)。
  M2 より先に行う: 実体化と pin の形が決まらないと、global GC が in-flight メッセージを
  どう扱うか(pin の付け直し先)を確定できないため。
- **M2 global GC**: バリア、root 表(local と共有)、全 objspace の一括 mark/sweep、
  shref_bits の再計算、global の起動条件(shareable 増加・滞留・zombie Ractor)。
- **M4 終了と引き継ぎ**: join 時のその場併合、未 join の global GC 内 main 併合、
  終了済み Ractor の一覧延命、finalizer / zombie の引き継ぎ実行、単一 Ractor 復帰時の
  shareable 回収、fork / shutdown の接続。
- **M1b local GC の並行化(バリア外し)**: gc_enter / gc_exit から VM lock と barrier を
  外し、§2.1 本文どおり「local GC はロックもバリアも取らない」を実現する。
  **並列性能はここで出る**(それまでは GC が STW なので、GC を踏む負荷では master と
  同等止まり)。最後尾に置く理由:
  1. **競合面を開くのは一度だけにする。** M1a〜M4 の green は「GC 中は誰も動かない」
     前提で検証されている。バリアを外すと、バリアが隠していた競合面(§1.4 の
     ビットマップ書き込み、§2.4 の VM 共有構造、§4.2 の in-flight メッセージの読み書き)が
     一斉に表に出る。ここは経験的に最もバグ密度が高く、再現が確率的で TSan でしか
     根本原因に辿れない領域なので、機能追加と混ぜずに単独で開けたい。
  2. **手戻りの回避。** M2 / M4 は「全 Ractor を止めて全 objspace を見る・併合する」
     STW コードであり、M1b の有無にほぼ影響されない。逆に M1b を先にやると、M2 / M4 を
     足すたびに「並行中の local GC と global GC の遷移プロトコル」を再設計・再検証する
     ことになり、いちばん高い検証コストを複数回払う。
  3. **遷移プロトコルは global GC が無いと設計できない。** 「global 開始時に進行中の
     local GC を完走させてから全停止する」「local 側は global の進行中フラグを見て退避する」
     といった排他は、相手(M2)が存在して初めて書ける・試せる。M1b が先だと、その時点の
     並行性検証は M2 導入で陳腐化する。
  4. **機能完結が先。** M2 / M4 が無い間は shareable と終了 Ractor の objspace が
     貯まり続ける。その状態で性能を測っても「リークする処理系の速度」になり、
     チューニングの判断材料にならない。
  なお M0 時点で master と同等性能(割り当て経路に退行なし)は確認済み。性能の伸び代は
  M1b 完了後にまとめて測り直す。
- **M5 堅牢化・調整**: 既知のクラッシュ再現スクリプト群(`rlgc_repro/`)と多 Ractor
  ストレスシナリオをテストオラクルに常用する。btest / test-all に加え、**ASAN / TSan を
  CI に常設**する(並行 GC のバグは再現が確率的で、サニタイザでないと根本原因まで
  辿れない — 特に M1b の検証は TSan が主武器)。GC_STRESS はタイミングを変えて
  昇格依存のバグを隠すことがあるので、stress あり / なしの両方を回す。global GC
  起動条件の係数・下限(§2.2)と born-shareable の increment 箇所の網羅
  (クラス生成・fstring・freeze 等)もここで確定。
