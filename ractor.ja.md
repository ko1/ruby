# まとめ

* 1つのRactorは並行実行単位として実行される
  * 1つのRactorは1つ以上のスレッドをもつ
  * スレッドは各 Ractor に属するグローバルロックで共有される
* Ractor へメッセージを送受信しながら同期して実行をすすめる
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
  * `Ractor.new` に渡した引数が、incoming メッセージとしてブロックパラメータで受け取る
  * ブロックの返値が、outgoing メッセージとなる

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

* Ractor 間でオブジェクトが共有されないように、「ブロックの外側のローカル変数（など）」「self」は隔離される

* 与えられたブロックは、`Proc#isolate` によって外側の環境にアクセスできない
* エラーは `Proc#isolate` が実行された瞬間に起こる。つまり `Ractor.new` したときに起こる

```ruby
  begin
    a = true
    r = Ractor.new do
      a #=> ArgumentError
    end
    r.recv # Ractor の実行を待つ。後述
  rescue ArgumentError
  end
```

* 与えられたブロックの `self` は、その Ractor オブジェクト自身になる（外側の `self` とは別になる）

```ruby
  r = Ractor.new do
    self.object_id
  end
  r.recv == self.object_id #=> true
```

* `Ractor.new` に渡された（キーワード引数以外の）引数は、ブロックの引数になる。ただし、参照を渡すのでは無く、その Ractor へのincoming メッセージとなる（コピーになる。詳細は後述）

```ruby
  r = Ractor.new 'ok' do |msg|
    msg #=> 'ok'
  end
  r.recv #=> 'ok'
```

```ruby
  # 上のコードとほぼ同じ意味
  r = Ractor.new do
    msg = Ractor.recv
    msg
  end
  r.send 'ok'
  r.recv #=> 'ok'
```

* ブロックの返値は、その Ractor からの outgoing メッセージとなる（コピーになる。詳細は後述）

```ruby
  r = Ractor.new do
    'ok'
  end
  r.recv #=> `ok`
```

```ruby
  # 上のコードとほぼ同じ意味
  r = Ractor.new do
    Ractor.send 'ok'
  end
  r.recv #=> 'ok'
```

* ブロックのエラー値は、outgoing メッセージを受信した Ractor 上でエラーが伝搬する

```ruby
  r = Ractor.new do
    raise 'ok' # exception will be transferred receiver
  end
  begin
    r.recv
  rescue Ractor::RemoteError => e
    e.cause.class   #=> RuntimeError
    e.cause.message #=> 'ok'
    e.ractor        #=> r
  end
```

# Ractor 間のコミュニケーション

* Ractor 間のコミュニケーションは、次の 2 つの方法がある。
  * (1) Ractor と直接送受信する（カテゴリわけのない Mailbox）
  * (2) 共有可能なコンテナオブジェクトを用いる（未実装）
* 同期・待ち合わせは、基本的には Ractor 間の通信を用いる
* (1) は、内部的に双方向チャンネル incoming channel, outgoing channel を持つ
* (2) は、データの送受信は行うことができるが、待ち合わせには用いない（... 多分）
* まだ送信されていないチャンネルから `recv` しようとすると、何か来るまで待つ（タイムアウトはまだ実装していない）
* 複数の受信を同時に待つ `Ractor.select(ractors...)` がある。
* チャンネルは close することができる
  * `close` されたチャンネルから recv してブロックしようとすると、例外（未受信だと例外）
  * `close` されたチャンネルへ send しようとすると例外
  * `Ractor#close_incoming` および `Ractor#close_outgoing` がある
  * Ractor が終了すると、その Ractor の incoming/outgoing channel はそれぞれ `close` される
* Ractror 間送受信において、メッセージとしてオブジェクトを送受信する方法には、コピーと移動の2種類がある
  * コピー：すべてコピーして送る `send`
  * 移動：送信元で、以降一切用いないことを前提に転送を高速化する `move`
  * 受信には `recv` しかない
* 共有可能オブジェクトは、send、move 関係なく参照のみ送られる。

## Ractor 間の送受信

* 各 Ractor は、それぞれ _incoming-channel_、_outgoing-channel_ を持つ
* `Ractor#send`、`Ractor#recv` は、それぞれ incoming channel への送信、outgoing channel からの受信となる
* `Ractor.recv`、`Ractor.send` は、それぞれ incoming channel からの受信、outgoing channel への送信となる

```
Ractor 外                                   Ractor 内
                           |
  Ractor#send  ---- incoming channel ----> Ractor.recv
  Ractor#recv <---- outgoing channel ----  Ractor.send
                           |
```

```ruby
  r = Ractor.new do
    msg = Ractor.recv # g の incoming channel からの受信
  end
  r.send 'ok' # g の incoming channel へ送信
  r.recv      # g の outgoing channel から受信
```

```ruby
  # 実引数は incoming channel への送信
  r = Ractor.new 'ok' do |msg|
    # 仮引数は incoming channel からの受信

    msg # ブロックの返値は outgoing channel への送信
  end

  # g の outgoing channel からの受信
  r.recv #=> `ok`
```

* 複数の Ractor が一つの Ractor に対して待ち合わせが可能（`Ractor.select`）

```ruby
  pipe = Ractor.new do
    loop do
      Ractor.send Ractor.recv
    end
  end

  RN = 10
  rs = RN.times.map{|i|
    Ractor.new pipe, i do |pipe, i|
      msg = pipe.recv
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
      Ractor.send Ractor.recv
    end
  end

  RN = 10
  rs = RN.times.map{|i|
    Ractor.new pipe, i do |pipe, i|
      pipe << i
    end
  }
  RN.times.map{
    pipe.recv
  }.sort #=> [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
```

## `Ractor.select` で複数の Ractor から recv する

* `Ractor.select(*ractors)` を用いて複数のチャンネルから待機できる
* 返値は、「どのチャンネル（もしくは Ractor）から送信されたか」、「送信されたオブジェクト」の2つ

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
* go-lang の `select` syntax は、同時に受信可能なチャンネルがある場合、ランダム（ラウンドロビン？）に選択するするらしく、こちらもそのようにしたほうが良いと思われる（現在は、最初に見つけた受信可能チャンネル）

## チャンネルの端点を close

* `Ractor#close_incoming/outgoing` でチャンネルを close することができる（`Queue#close` と同じ）
* `close` されたチャンネルから recv してブロックしようとするとエラー
* Ractor が終了すると、outgoing channel が自動的に `close` される

```ruby
  r = Ractor.new do
    'finish'
  end
  r.recv
  begin
    o = r.recv
  rescue Ractor::ClosedError
    'ok'
  else
    "ng: #{o}"
  end
```

* `close` されたチャンネルへ send しようとするとエラー
* Ractor が終了すると、incoming channel が自動的に `close` される

```ruby
  r = Ractor.new do
  end

  r.recv # wait terminate

  begin
    r.send(1)
  rescue Ractor::ClosedError
    'ok'
  else
    'ng'
  end
```

* `close` を使って、複数 Ractor へ一斉に close されたという情報を発信することができる

```
TODO: 例
```

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

* `Ractor::move(obj)`は、`obj`が共有不可能オブジェクトであれば、move する
* move されたオブジェクトは、送信元 Ractor で参照しようとすると(例えば、メソッド呼び出し）、エラーになる

```ruby
  # move
  r = Ractor.new do
    obj = Ractor.recv
    obj << ' world'
  end

  str = 'hello'
  r.move str
  modified = r.recv #=> 'hello world'

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
      Ractor.send v
    end
  end

  class C
  end

  sharable_objects = [1, :sym, 'xyzzy'.to_sym, 'frozen'.freeze, 1+2r, 3+4i, /regexp/, C]

  sr = sharable_objects.map{|o|
    r << o
    o2 = r.recv
    [o, o.object_id == o2.object_id]
  }
  #=> [[1, true], [:sym, true], [:xyzzy, true], [\"frozen\", true], [(3/1), true], [(3+4i), true], [/regexp/, true], [C, true]]

  ur = unsharable_objects = ['mutable str'.dup, [:array], {hash: true}].map{|o|
    r << o
    o2 = r.recv
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
    r.recv
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
    r.recv
  rescue => e
    e.class #=> RuntimeError
  end
```

```ruby
  ch = Ractor::Channel.new
  ch.instance_variable_set(:@iv, 'str')

  r = Ractor.new ch do |ch|
    p ch.instance_variable_get(:@iv)
  end

  begin
    r.recv
  rescue => e
    e.class #=> RuntimeError
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
    r.recv
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
    r.recv
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
    r.recv
  rescue => e
    e.class
  end
```

# その他実装

* まだ並列化していない（実は全部従来の GVL 使っている）
* デバッグモード
  * 生成時に Ractor ID（uint32_t、連番）を振り、VM push 時に現 Ractor ID と異なれば rb_bug()

# 悩んでいること

* Channel の取り扱い
  * recv する Ractor が居る、というのを陽に示したい（いなくなったら close して欲しい）
  * Channel を見えるようにするのでは無く、recv_port, send_port のように（pipe の read/write みたいに）扱う方が良いか？　その Ractor が send/recv するというのをどうやって表明させるか？



