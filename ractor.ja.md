# まとめ

* 1つのRactorは並行実行単位として実行される
  * 1つのRactorは1つ以上のスレッドをもつ
  * スレッドは各 Ractor に属するグローバルロックで共有される
* Ractor へメッセージを送受信（message passing）しながら同期して実行をすすめる
  * push 型（sender knows receiver）（actor model）の `Ractor#send` + `Ractor.recv`
  * pull 型（receiver knows sender）の `Ractor.yield` + `Ractor#take`
* メッセージは Ruby のオブジェクト
  * オブジェクトは共有可能・不可能オブジェクトに二分され、共有不可能オブジェクトはたかだか一つの Ractor からしか参照されない
  * 共有可能オブジェクトをメッセージとして転送すると、単に参照が送られる
  * 共有不可能オブジェクトをメッセージとして転送すると、原則コピーされる
  * 共有不可能オブジェクトは、（ほぼ）コピーしない move ができるものがあるが、転送元からは参照できなくなる
* 共有可能オブジェクトへのアクセスは必ず排他制御される
  * C レベルでの SEGV は起こらない
  * ただし、トランザクションが足りないことがあるため、レースは発生しうる。例えば、トランザクションをまたいだ変更など。

# Ractor の生成と終了

* `Ractor.new do ... end` で Ractor を生成する
* 渡したブロックが新しい Ractor 上で実行される
  * ブロックは外側の環境と隔離される
  * `Ractor.new` に渡した引数は incoming message としてブロックパラメータに到達する
  * ブロックの返値が、outgoing message となる

## Ractor の生成

* `Ractor.new` メソッドで Ractor 作成
* 渡したブロックが生成された Ractor で並行実行される

```ruby
Ractor.new do
  # このブロックが並行に実行される
end
```

* `name:` で名前を与えられる

```ruby
  r = Ractor.new name: 'test-name' do
  end
  r.name #=> 'test-name'
```

## Ractor に渡したブロックは、生成側の環境からは隔離される

* Ractor 間でオブジェクトが共有されないように、「ブロックの外側のローカル変数（など）」と「self」は隔離される
  * 与えられたブロックは、`Proc#isolate` によって外側の環境にアクセスできない
  * エラーは `Proc#isolate` が実行された瞬間に起こる。つまり `Ractor.new` したときに起こる

```ruby
  begin
    a = true
    r = Ractor.new do
      a #=> ArgumentError
    end
    r.take # Ractor の実行を待つ。後述
  rescue ArgumentError
  end
```

* 与えられたブロックの `self` は、その Ractor オブジェクト自身になる（外側の `self` とは別になる）

```ruby
  r = Ractor.new do
    self.object_id
  end
  r.take == self.object_id #=> true
```

* `Ractor.new` に渡された（キーワード引数以外の）引数は、ブロックの引数になる。ただし、参照を渡すのでは無く、その Ractor へのincoming messageとなる（詳細は後述）

```ruby
  r = Ractor.new 'ok' do |msg|
    msg #=> 'ok'
  end
  r.take #=> 'ok'
```

```ruby
  # 上のコードとほぼ同じ意味
  r = Ractor.new do
    msg = Ractor.recv
    msg
  end
  r.send 'ok'
  r.take #=> 'ok'
```

* ブロックの返値は、その Ractor からの outgoing message となる（詳細は後述）

```ruby
  r = Ractor.new do
    'ok'
  end
  r.take #=> `ok`
```

```ruby
  # 上のコードとほぼ同じ意味
  r = Ractor.new do
    Ractor.yield 'ok'
  end
  r.take #=> 'ok'
```

* ブロックのエラー値は、outgoing message を受信した Ractor 上でエラーが伝搬する

```ruby
  r = Ractor.new do
    raise 'ok' # exception will be transferred receiver
  end
  begin
    r.take
  rescue Ractor::RemoteError => e
    e.cause.class   #=> RuntimeError
    e.cause.message #=> 'ok'
    e.ractor        #=> r
  end
```

# Ractor 間のコミュニケーション

* Ractor 間のコミュニケーションは、メッセージパッシングと、共有可能コンテナオブジェクトによって行う
  * (1) メッセージパッシング
    * (1-1) push 型の send/recv（send する側が宛先を知っている） aka actor model
    * (1-2) pull 型の yield/take（take する側が送信元を知っている） aka ランデブー
  * (2) 共有可能なコンテナオブジェクトを用いる（未実装）
* 待ち合わせ
  * 待ち合わせは、基本的に (1) メッセージパッシングで行う
  * (2) は、データの送受信は行うことができるが、待ち合わせには用いない（... 多分）
* (1-1) send/recv（push 型通信？）
  * `Ractor#send`（`Ractor#<<` が alias）は、対象 Ractor の incoming port へメッセージを送信する。incoming port は無限サイズの incoming queue に接続されているので、`Ractor#send` はブロックしない。
  * `Ractor.recv` で、自 Ractor の incoming queue からメッセージを一つ取り出す。incoming queue が空ならブロックする
* (1-2) yield/take（pull 型通信？）
  * `Ractor.yield(obj)` でメッセージを `Ractor#taks` している Ractor へ送信する
  * どちらも、相手が発生するまでブロックする
* `Ractor.select()` で、take, recv, yield のどれかが成功するまで待つことができる
* port は close することができる
  * `Ractor#close_incoming` および `Ractor#close_outgoing` がある
  * incoming port を close すると、それ以降 send することができない。また、空の incoming queue を待っていた場合、例外になる
  * outgoing port を close すると、`take` もしくは `yield` ができなくなる。もし、待っているものがいた場合、例外になる
  * Ractor が終了すると、その Ractor の incoming/outgoing port はそれぞれ `close` される
* Ractror 間送受信において、メッセージとしてオブジェクトを送受信する方法は、次の3種類
  * (1) 参照：共有可能オブジェクトは、参照のみ送る（速い）
  * (2) コピー：すべてコピー（ディープコピー）して送る
  * (3) 移動：送信元で、以降一切用いないことを前提に軽量なコピーを送る
  * 移動したい場合、`send` もしくは `yield` に `move: true` オプションを付けて指定する

## Ractor 間の送受信

* 各 Ractor は、それぞれ _incoming-port_、_outgoing-port_ を持つ
* incoming port には無限サイズのキューである incoming queue が接続されている

```
                  Ractor r
                 +-------------------------------------------+
                 | incoming                         outgoing |
                 | port                                 port |
   r.send(obj) --*--[incoming queue]     Ractor.yield(obj) --*-- r.take
                 |                |                          |
                 |                v                          |
                 |           Ractor.recv                     |
                 +-------------------------------------------+


接続することができる（r2.send obj on r1、Ractor.recv on r2）
  +----+     +----+
  * r1 |-----* r2 *
  +----+     +----+


接続することができる（Ractor.yield(obj) on r1, r1.take on r2）
  +----+     +----+
  * r1 *------ r2 *
  +----+     +----+

同時に待つことができる（Ractor.select(r1, r2)）
  +----+
  * r1 *------+
  +----+      |
              +----- Ractor.select(r1, r2)
  +----+      |
  * r2 *------|
  +----+

```

```ruby
  r = Ractor.new do
    msg = Ractor.recv # r の incoming queue からの受信
  end
  r.send 'ok' # r の incoming port -> incoming queue へ送信
  r.take      # r の outgoing port から受信
```

```ruby
  # 実引数は incoming queue への送信
  r = Ractor.new 'ok' do |msg|
    # 仮引数は incoming queue からの受信

    msg # ブロックの返値は outgoing port への送信
  end

  # g の outgoing port からの受信
  r.take #=> `ok`
```

* 複数の Ractor が一つの Ractor に対して待ち合わせが可能（`Ractor.select`）

```ruby
  pipe = Ractor.new do
    loop do
      Ractor.yield Ractor.recv
    end
  end

  RN = 10
  rs = RN.times.map{|i|
    Ractor.new pipe, i do |pipe, i|
      msg = pipe.take
      msg # ping-pong
    end
  }
  RN.times{|i|
    pipe << i
  }
  RN.times.map{
    r, n = Ractor.select(*rs)
    rs.delete r
    n
  }.sort #=> [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
```

* 複数の Ractor が一つの Ractor に送信可能

```ruby
  pipe = Ractor.new do
    loop do
      Ractor.yield Ractor.recv
    end
  end

  RN = 10
  rs = RN.times.map{|i|
    Ractor.new pipe, i do |pipe, i|
      pipe << i
    end
  }
  RN.times.map{
    pipe.take
  }.sort #=> [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
```

## `Ractor.select` で複数の Ractor から recv する

* `Ractor.select(*ractors)` を用いて複数の Ractor からの take を待つことができる
* 返値は、「どの Ractor からメッセージが届いたのか」、「送信されたオブジェクト」の2つ

```ruby
  r1 = Ractor.new{'r1'}
  r, obj = Ractor.select(r1)
  r == r1 and obj == 'r1' #=> true
```

```ruby
  # select 2
  r1 = Ractor.new{'r1'}
  r2 = Ractor.new{'r2'}
  rs = [r1, r2]
  as = []
  r, obj = Ractor.select(*rs)
  rs.delete(r)
  as << obj
  r, obj = Ractor.select(*rs)
  rs.delete(r)
  as << obj
  as.sort == ['r1', 'r2'] #=> true
```

* `select(2)` と同じ C10K problem があるので、その辺なんとかしたい（良い感じの API）
* go-lang の `select` syntax は、同時に受信可能なチャンネルがある場合、ランダム（ラウンドロビン？）に選択するするらしく、こちらもそのようにしたほうが良いと思われる（現在は、引数の順序通りに見ていく）

## Ractor の port を close

* `Ractor#close_incoming/outgoing` で incoming/outgoing port を close することができる（`Queue#close` と同じ）
* `close_incoming` された Ractor に `Ractor#send`すると例外。incoming queue が空のとき（ブロックしようとするとき） `Ractor.recv` すると例外
* `close_outgoing` された Ractor で `Ractor.yield` する、もしくは `Ractor#take` すると例外
* Ractor が終了すると、incoming/outgoing port が自動的に `close` される

```ruby
  r = Ractor.new do
    'finish'
  end
  r.take
  begin
    o = r.take
  rescue Ractor::ClosedError
    'ok'
  else
    "ng: #{o}"
  end
```

```ruby
  r = Ractor.new do
  end

  r.take # wait terminate

  begin
    r.send(1)
  rescue Ractor::ClosedError
    'ok'
  else
    'ng'
  end
```

* 複数の Ractor が一つの Ractor の yield を待っているとき、`Ractor#close_outgoing` すると yield 待ちがすべてキャンセルされる（ClosedError）

## コピーによるオブジェクトの転送

* `Ractor::send(obj)` は、`obj` が共有不可能オブジェクトであれば、(deep) コピーする

```ruby
  obj = 'str'.dup
  r = Ractor.new obj do |msg|
    msg.object_id
  end
  
  obj.object_id == r.recv #=> false
```

* 現状は `Marshal#dump` し、`recv` 側で `load` する（dRuby と同じ）。なので、対応しないオブジェクトは送ることができない。
* 専用のコピーコードを書き下す必要がある

```ruby
  obj = Thread.new{}
  begin
    r = Ractor.new obj do |msg|
      msg
    end
  rescue TypeError => e
    e.message #=> no _dump_data is defined for class Thread
  else
    'ng'
  end
```

## move によるオブジェクトの転送

* `Ractor::send(obj, move: true)`は、`obj`が共有不可能オブジェクトであれば、move する
* move されたオブジェクトは、送信元 Ractor で参照しようとすると(例えば、メソッド呼び出し）、エラーになる

```ruby
  # move
  r = Ractor.new do
    obj = Ractor.recv
    obj << ' world'
  end

  str = 'hello'
  r.move str
  modified = r.take #=> 'hello world'

  begin
    # move した文字列を触ろうとするのでエラー
    str << ' exception' # raise Ractor::MovedError
  rescue Ractor::MovedError
    modified #=> 'hello world'
  else
    raise 'unreachable'
  end
```

* 現状では、`T_FILE`、`T_STRING`、`T_ARRAY` にのみ対応する
  * `T_FILE` はソケットなどを念頭に（サーバアプリ）
  * `T_STRING` は、コピーではないのでバッファの確保が不要になって速い（でかい場合）
  * `T_ARRAY` もバッファの確保が不要になる。ただし、全要素をなめる必要があるので、速くはない（多分、あまり使われない）
* アクセス禁止の実装は、禁断のクラスの差し替えによる

# 共有可能オブジェクト

* 次のオブジェクトが Ractor 間で（現状）共有可能
  * `SPECIAL_CONST_P()`
  * native に frozen な Numeric と Symbol
    * `T_FLOAT`、`T_COMPLEX`、`T_RATIONAL`, `T_BIGNUM`
    * `T_SYMBOL`
  * frozen な `T_STRING` と `T_REGEXP`
    * ただし、ivar がない（`FL_EXIVAR` がない）
  * クラス、モジュール：`T_CLASS`、`T_MODULE`、`T_ICLASS`
  * `Ractor` などの共有を前提としたデータ構造
* 将来的には、immutable なコンテナなども対象に
  * deep frozen な Array, Hash など → FL_IMMUTABLE 作る？
* 共有可能な !special const オブジェクトは `FL_SHAREABLE` がつく
  * frozen な String など、あとで調査したときに付ける

```ruby
  r = Ractor.new do
    while v = Ractor.recv
      Ractor.yield v
    end
  end

  class C
  end

  sharable_objects = [1, :sym, 'xyzzy'.to_sym, 'frozen'.freeze, 1+2r, 3+4i, /regexp/, C]

  sr = sharable_objects.map{|o|
    r << o
    o2 = r.take
    [o, o.object_id == o2.object_id]
  }
  #=> [[1, true], [:sym, true], [:xyzzy, true], [\"frozen\", true], [(3/1), true], [(3+4i), true], [/regexp/, true], [C, true]]

  ur = unsharable_objects = ['mutable str'.dup, [:array], {hash: true}].map{|o|
    r << o
    o2 = r.take
    [o, o.object_id == o2.object_id]
  }
  #+> "[[\"mutable str\", false], [[:array], false], [{:hash=>true}, false]]]"
```

# 共有不可オブジェクトを共有させないために

* グローバル変数は main Ractor でのみ利用可能

```ruby
  $gv = 1
  r = Ractor.new do
    $gv
  end

  begin
    r.take
  rescue Ractor::RemoteError => e
    e.cause.message #=> 'can not access global variables from non-main Ractors'
  end
```

* outer-local variable は参照不可（`Proc#isolate`）

```ruby
  begin
    a = true
    r = Ractor.new do
      a
    end
  rescue => e
    e.class #=> ArgumentError
  end
```

* 共有可能オブジェクトのインスタンス変数は、main Ractor（最初に生成されたオブジェクト）からのみアクセス可

```ruby
  class C
    @iv = 'str'
  end

  r = Ractor.new do
    class C
      p @iv
    end
  end


  begin
    r.take
  rescue => e
    e.class #=> RuntimeError
  end
```

```ruby
  shared = Ractor.new{}
  shared.instance_variable_set(:@iv, 'str')

  r = Ractor.new shared do |shared|
    p shared.instance_variable_get(:@iv)
  end

  begin
    r.take
  rescue Ractor::RemoteError => e
    e.cause.message
  end
```

* クラス変数も main Ractor からのみアクセス可
* 利用しているライブラリは対応が必要

```ruby
  class C
    @@cv = 'str'
  end

  r = Ractor.new do
    class C
      p @@cv
    end
  end


  begin
    r.take
  rescue => e
    e.class #=> RuntimeError
  end
```

* 共有不可オブジェクトが格納されている定数の参照は、main ractor からのみ可

```ruby
  class C
    CONST = 'str'
  end
  r = Ractor.new do
    C::CONST
  end
  begin
    r.take
  rescue => e
    e.class #=> NameError
  end
```

* 共有不可オブジェクトを定数にセットするのは、main Ractor からのみ可

```ruby
  class C
  end
  r = Ractor.new do
    C::CONST = 'str'
  end
  begin
    r.take
  rescue => e
    e.class
  end
```

# 検討

* channel で通信しないのは、エラー伝搬を確実に行うため
  * Close した（死亡した）Ractor に send
  * Close した（死亡した）Ractor から take
  * Close した（おそらく外部から close された）incoming port から recv
  * Close した（このケースはあるんだろうか...?）outgoing port へ yield
  * これで、多分 take で結果を受け取るのであれば、間違いに気づくことができる
* エラー伝搬が起こらないケース
  * 誰も待っていないのに yield ... これはなんとかなるんだろうか？　無視すればいい？
  * 誰も送ってくれないのに recv（send する側が死亡した場合）→ 結果は take で待つという文化になるか？
* `take` は、Erlang における `link` を Ruby でどうするといいかなと検討した結果（能動的な監視）、その発展

# その他実装

* まだ並列化していない（実は全部従来の GVL 使っている）
* デバッグモード
  * 生成時に Ractor ID（uint32_t、連番）を振り、VM push 時に現 Ractor ID と異なれば rb_bug()

# Examples

## ring in actor model

```
RN = 10000
CR = Ractor.current

last_r = r = Ractor.new do
  p Ractor.recv
  CR << :fin
end

RN.times{
  r = Ractor.new r do |next_r|
    next_r << Ractor.recv
  end
}

p :setup_ok
r << 1
p Ractor.recv
```

## fork-join

```
def fib n
  if n < 2
    1
  else
    fib(n-2) + fib(n-1)
  end
end

RN = 10
rs = (1..RN).map do |i|
  Ractor.new i do |i|
    [i, fib(i)]
  end
end

until rs.empty?
  r, v = Ractor.select(*rs)
  rs.delete r
  p answer: v
end
```

## worker pool

```
require 'prime'

pipe = Ractor.new do
  loop do
    Ractor.yield Ractor.recv
  end
end

N = 1000
RN = 10
workers = (1..RN).map do
  Ractor.new pipe do |pipe|
    while n = pipe.take
      Ractor.yield [n, n.prime?]
    end
  end
end

(1..N).each{|i|
  pipe << i
}

pp (1..N).map{
  r, (n, b) = Ractor.select(*workers)
  [n, b]
}.sort_by{|(n, b)| n}
```

