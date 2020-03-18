# まとめ

* 1つのGuildは並行実行単位として実行される
  * 1つのGuildは1つ以上のスレッドをもつ
  * スレッドは各 Guild に属するグローバルロックで共有される
* Guild 間はチャンネルを用いて通信・同期を行いながら実行する
* オブジェクトは共有可能・不可能オブジェクトに二分され、共有不可能オブジェクトはたかだか一つの Guild からしか参照されない
* 共有可能オブジェクトへのアクセスは必ず排他制御される
  * C レベルでの SEGV は起こらない
  * ただし、トランザクションが足りないことがあるため、レースは発生しうる。例えば、トランザクションをまたいだ変更など。

# Guild の生成と終了

* `Guild.new do ... end` で Guild を生成する
* 渡したブロックが新しい Guild 上で実行される
  * ブロックは外側の環境と隔離される
  * `Guild.new` に渡した引数が、incoming メッセージとしてブロックパラメータで受け取る
  * ブロックの返値が、outgoing メッセージとなる

## Guild の生成

* `Guild.new` メソッドで Guild 作成
* 渡したブロックが生成された Guild で並行実行される

```ruby
Guild.new do
  # このブロックが並行に実行される
end
```

* `name:` で名前を与えられる

```ruby
  g = Guild.new name: 'test-name' do
  end
  g.name #=> 'test-name'
```

## Guild に渡したブロックは、生成側の環境からは隔離される

* 与えられたブロックは、`Proc#isolate` によって外側の環境にアクセスできない
* エラーは `Proc#isolate` が実行された瞬間に起こる。つまり `Guild.new` したときに起こる

```ruby
  begin
    a = true
    g = Guild.new do
      a #=> ArgumentError
    end
    g.recv
  rescue ArgumentError
  end
```

* 与えられたブロックの `self` は、その Guild オブジェクト自身になる（外側の `self` とは別になる）

```ruby
  g = Guild.new do
    self.object_id
  end
  g.recv == self.object_id #=> true
```

* `Guild.new` に渡された（キーワード引数以外の）引数は、ブロックの引数になる。ただし、参照を渡すのでは無く、その Guild へのincoming メッセージとなる（コピーになる。詳細は後述）

```ruby
  g = Guild.new 'ok' do |msg|
    msg #=> 'ok'
  end
  g.recv
```

```ruby
  # 上のコードとほぼ同じ意味
  g = Guild.new do
    msg = Guild.recv
    msg
  end
  g.send 'ok'
  g.recv #=> 'ok'
```

* ブロックの返値は、その Guild からの outgoing メッセージとなる（コピーになる。詳細は後述）

```ruby
  g = Guild.new do
    'ok'
  end
  g.recv #=> `ok`
```

```ruby
  # 上のコードとほぼ同じ意味
  g = Guild.new do
    Guild.send 'ok'
  end
  g.recv #=> 'ok'
```

* ブロックのエラー値は、outgoing メッセージを受信した Guild 上でエラーが伝搬する

```ruby
  g = Guild.new do
    raise 'ok' # exception will be transferred receiver
  end
  begin
    g.recv
  rescue Guild::RemoteError => e
    e.cause.class   #=> RuntimeError
    e.cause.message #=> 'ok'
  end
```

# Guild 間のコミュニケーション

* Guild 間のコミュニケーションは、次の3つの方法がある。
  * (1) Guild と直接送受信する（カテゴリわけのない Mailbox）
  * (2) `Guild::Channel` を用いる（go とかと同じ）
  * (3) 共有可能なコンテナオブジェクトを用いる（未実装）
* 同期・待ち合わせは、基本的には `Guild::Channel` を用いる
* (1) の Guild と直接送受信する、というのは、`Guild::Channel` を内部で用いている
* (3) は、データの送受信は行うことができるが、待ち合わせには用いない。
* まだ送信されていないチャンネルから `recv` しようとすると、何か来るまで待つ（タイムアウトはまだ実装していない）
* 複数のチャンネルを同時に待つ `Guild.select(channels...)` がある。
* チャンネルは close することができる
  * `close` されたチャンネルから recv してブロックしようとすると、例外（未受信だと例外）
  * `close` されたチャンネルへ send しようとすると例外
  * Guild が終了すると、その Guild の incoming/outgoing channel はそれぞれ `close` される
* チャンネル間送受信において、送るオブジェクトはコピーと移動の2種類がある
  * すべてコピーして送る `send`
  * 送信元で、以降一切用いないことを前提に転送を高速化する `move`
  * 受信には `recv` しかない
* 共有可能オブジェクトは、send、move 関係なく参照のみ送られる。

## Guild と直接送受信する

* 各 Guild は、それぞれ _incoming-channel_、_outgoing-channel_ を持つ
* `Guild#send`、`Guild#recv` は、それぞれ incoming channel への送信、outgoing channel からの受信となる
* `Guild.recv`、`Guild.send` は、それぞれ incoming channel からの受信、outgoing channel への送信となる

```
Guild 外                                   Guild 内
                           |
  Guild#send  ---- incoming channel ----> Guild.recv
  Guild#recv <---- outgoing channel ----  Guild.send
                           |
```

```ruby
  g = Guild.new do
    msg = Guild.recv # g の incoming channel からの受信
  end
  g.send 'ok' # g の incoming channel へ送信
  g.recv      # g の outgoing channel から受信
```

```ruby
  # 実引数は incoming channel への送信
  g = Guild.new 'ok' do |msg|
    # 仮引数は incoming channel からの受信

    msg # ブロックの返値は outgoing channel への送信
  end

  # g の outgoing channel からの受信
  g.recv #=> `ok`
```

## `Guild::Channel` を用いて送受信する

* `Guild::Channel` を用いて送受信可能

```ruby
  ch = Guild::Channel.new
  g = Guild.new ch do |ch|
    ch.recv #=> 'ok'
  end

  ch << 'ok' # << は send の alias
  ch.recv
```

* 複数の Guild が一つの Channel に待ち合わせが可能

```ruby
  ch = Guild::Channel.new
  GN = 10
  gs = GN.times.map{|i|
    Guild.new ch, i do |ch, i|
      # 複数の Guild が ch で待ち合わせ
      msg = ch.recv
      msg # ping-pong
    end
  }
  GN.times{|i|
    # Guild の数だけ仕事を与える
    ch << i
  }
  rs = GN.times.map{
    # Guild.select は後述
    g, n = Guild.select(*gs)
    gs.delete g
    n
  }.sort #=> [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
```

* 複数の Guild が一つの Channel に送信可能

```ruby
  ch = Guild::Channel.new
  GN = 10
  gs = GN.times.map{|i|
    Guild.new ch, i do |ch, i|
      # 一つの ch に、複数の Guild からオブジェクトを転送
      ch << i
    end
  }
  rs = GN.times.map{
    ch.recv
  }.sort #=> [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
```

## `Guild.select` で複数のチャンネルから recv する

* `Guild.select(*channels)` を用いて複数のチャンネルから待機できる
* もし、引数に Guild が指定されたとき、その Guild の outgoing-channel が指定されたとみなす
* 返値は、「どのチャンネル（もしくは Guild）から送信されたか」、「送信されたオブジェクト」の2つ

```ruby
  g1 = Guild.new{'g1'}
  g, obj = Guild.select(g1)
  g == g1 and obj == 'g1' #=> true
```

```ruby
  # select 2
  g1 = Guild.new{'g1'}
  g2 = Guild.new{'g2'}
  gs = [g1, g2]
  rs = []
  g, obj = Guild.select(*gs)
  gs.delete(g)
  rs << obj
  g, obj = Guild.select(*gs)
  gs.delete(g)
  rs << obj
  rs.sort == ['g1', 'g2'] #=> true
```

* `Guild.select` で Guild の incoming channel への送信を待つ方法はない。要るかなぁ？
* `select(2)` と同じ C10K problem があるので、その辺はなんとかしたい。毎回 delete するのもちょっとダサいし。
* go-lang の `select` syntax は、同時に受信可能なチャンネルがある場合、ランダム（ラウンドロビン？）に選択するするらしく、こちらもそのようにしたほうが良いと思われる（現在は、最初に見つけた受信可能チャンネル）

## チャンネルの端点を close

* `Guild::Channel#close` でチャンネルを close することができる（`Queue#close` と同じ）
* `close` されたチャンネルから recv してブロックしようとするとエラー
* Guild が終了すると、outgoing channel が `close` される

```ruby
  # closed-channel (Guild)
  g = Guild.new do
    'finish'
  end
  g.recv
  begin
    o = g.recv
  rescue Guild::Channel::ClosedError
    'ok'
  else
    "ng: #{o}"
  end
```

* `close` されたチャンネルへ send しようとするとエラー
* Guild が終了すると、incoming channel が `close` される

```ruby
  g = Guild.new do
  end

  g.recv # wait terminate

  begin
    g.send(1)
  rescue Guild::Channel::ClosedError
    'ok'
  else
    'ng'
  end
```

* `close` を使って、複数 Guild へ一斉に close されたという情報を発信することができる

```ruby
  task_ch = Guild::Channel.new
  result_ch = Guild::Channel.new

  def work i, task
    Thread.pass
    "done #{task} by #{i}"
  end

  GN = 10
  TN = 1_000

  gs = (1..GN).map do |i|
    g = Guild.new i, task_ch, result_ch do |i, task_ch, result_ch|
      while task = task_ch.recv
        result_ch << work(i, task)
      end
    rescue Guild::Channel::ClosedError
      :ok
    end
  end

  TN.times{|i| task_ch << i}
  tn_results = TN.times.map{result_ch.recv}

  # close で、もう受信することができないことを 
  # worker guilds へ伝える
  task_ch.close

  gn_results = (1..GN).map{
    g, obj = Guild.select(*gs)
    gs.delete(g)
    obj
  }
```

## コピーによるオブジェクトの転送

* `Guild::Channel#send(obj)` は、`obj` が共有不可能オブジェクトであれば、(deep) コピーする

```ruby
  obj = 'str'.dup
  g = Guild.new obj do |msg|
    msg.object_id
  end
  
  obj.object_id == g.recv #=> false
```

* 現状は `Marshal#dump` し、`recv` 側で `load` する（dRuby と同じ）。なので、対応しないオブジェクトは送ることができない。

```ruby
  obj = Thread.new{}
  begin
    g = Guild.new obj do |msg|
      msg
    end
  rescue TypeError => e
    e.message #=> no _dump_data is defined for class Thread
  else
    'ng'
  end
```

## move によるオブジェクトの転送

* `Guild::Channel#move(obj)`は、`obj`が共有不可能オブジェクトであれば、move する
* move されたオブジェクトは、送信元 Guild で参照しようとすると(例えば、メソッド呼び出し）、エラーになる

```ruby
  # move
  g = Guild.new do
    obj = Guild.recv
    obj << ' world'
  end

  str = 'hello'
  g.move str
  modified = g.recv #=> 'hello world'

  begin
    # move した文字列を触ろうとするのでエラー
    str << ' exception' # raise Guild::Channel::Error
  rescue Guild::Channel::Error
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

* 次のオブジェクトが Guild 間で（現状）共有可能
  * `SPECIAL_CONST_P()`
  * native に frozen な Numeric と Symbol
    * `T_FLOAT`、`T_COMPLEX`、`T_RATIONAL`, `T_BIGNUM`
    * `T_SYMBOL`
  * frozen な `T_STRING` と `T_REGEXP`
    * ただし、ivar がない（`FL_EXIVAR` がない）
  * クラス、モジュール：`T_CLASS`、`T_MODULE`、`T_ICLASS`
  * `Guild`、`Guild::Channel` などの共有を前提としたデータ構造
* 将来的には、immutable なコンテナなども対象に
  * deep frozen な Array, Hash など → FL_IMMUTABLE 作る？
* 共有可能な !special const オブジェクトは `FL_SHAREABLE` がつく
  * frozen な String など、あとで調査したときに付ける

```ruby
  g = Guild.new do
    while v = Guild.recv
      Guild.send v
    end
  end

  class C
  end

  sharable_objects = [1, :sym, 'xyzzy'.to_sym, 'frozen'.freeze, 1+2r, 3+4i, /regexp/, C]

  sr = sharable_objects.map{|o|
    g << o
    o2 = g.recv
    [o, o.object_id == o2.object_id]
  }
  #=> [[1, true], [:sym, true], [:xyzzy, true], [\"frozen\", true], [(3/1), true], [(3+4i), true], [/regexp/, true], [C, true]]

  ur = unsharable_objects = ['mutable str'.dup, [:array], {hash: true}].map{|o|
    g << o
    o2 = g.recv
    [o, o.object_id == o2.object_id]
  }
  #+> "[[\"mutable str\", false], [[:array], false], [{:hash=>true}, false]]]"
```

# 共有不可オブジェクトを共有させないために

* グローバル変数は Guild local

```ruby
  $gv = 1
  g = Guild.new do
    $gv = 0 # $g is guild local variable.
    $gv
  end
  [g.recv, $gv] #=> [0, 1]
```

* outer-local variable は参照不可（`Proc#isolate`）

```ruby
  begin
    a = true
    g = Guild.new do
      a
    end
  rescue => e
    e.class #=> ArgumentError
  end
```

* 共有可能オブジェクトのインスタンス変数は、main Guild（最初に生成されたオブジェクト）からのみアクセス可

```ruby
  class C
    @iv = 'str'
  end

  g = Guild.new do
    class C
      p @iv
    end
  end


  begin
    g.recv
  rescue => e
    e.class #=> RuntimeError
  end
```

```ruby
  ch = Guild::Channel.new
  ch.instance_variable_set(:@iv, 'str')

  g = Guild.new ch do |ch|
    p ch.instance_variable_get(:@iv)
  end

  begin
    g.recv
  rescue => e
    e.class #=> RuntimeError
  end
```

* クラス変数も main Guild からのみアクセス可
* 利用しているライブラリは対応が必要

```ruby
  class C
    @@cv = 'str'
  end

  g = Guild.new do
    class C
      p @@cv
    end
  end


  begin
    g.recv
  rescue => e
    e.class #=> RuntimeError
  end
```

* 共有不可オブジェクトが格納されている定数の参照は、main guild からしかアクセス不可

```ruby
  class C
    CONST = 'str'
  end
  g = Guild.new do
    C::CONST
  end
  begin
    g.recv
  rescue => e
    e.class #=> NameError
  end
```

* 共有不可オブジェクトを定数にセットするのは、main Guild のみ

```ruby
  class C
  end
  g = Guild.new do
    C::CONST = 'str'
  end
  begin
    g.recv
  rescue => e
    e.class
  end
```

# その他実装

* まだ並列化していない（実は全部従来の GVL 使っている）
* デバッグモード
  * 生成時に Guild ID（uint32_t、連番）を振り、VM push 時に現 Guild ID と異なれば rb_bug()

# 悩んでいること

* Channel の取り扱い
  * recv する Guild が居る、というのを陽に示したい（いなくなったら close して欲しい）
  * Channel を見えるようにするのでは無く、recv_port, send_port のように（pipe の read/write みたいに）扱う方が良いか？　その Guild が send/recv するというのをどうやって表明させるか？



