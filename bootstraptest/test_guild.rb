# A return value of a guild block will be a
# message from the guild.
assert_equal 'ok', %q{
  # join
  g = Guild.new do
    'ok'
  end
  g.recv
}

# Passed arguments to Guild.new will be a block parameter
# The values are passed with guild-communication pass.
assert_equal 'ok', %q{
  # ping-pong with arg
  g = Guild.new 'ok' do |msg|
    msg
  end
  g.recv
}

assert_equal 'ok', %q{
  # ping-pong with two args
  g = Guild.new 'ping', 'pong' do |msg, msg2|
    [msg, msg2]
  end
  'ok' if g.recv == ['ping', 'pong']
}

# Guild#send passes an object with copy to a guild
# and Guild.recv in the guild block can receive the passed value.
assert_equal 'ok', %q{
  # ping-pong with channel
  g = Guild.new do
    msg = Guild.recv
  end
  g.send 'ok'
  g.recv
}

# communicate via Guild::Channel
assert_equal 'ok', %q{
  ch = Guild::Channel.new
  g = Guild.new ch do |ch|
    ch.recv #=> 'ok'
  end

  ch << 'ok'
  ch.recv
}

# Guild.select(*channels) receives a values from a channel.
# It is similar to select(2) and Go's select syntax.
# The return value is [ch, received_value]
assert_equal 'ok', %q{
  # select 1
  g1 = Guild.new{'g1'}
  g, obj = Guild.select(g1)
  'ok' if g == g1 and obj == 'g1'
}

assert_equal 'ok', %q{
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
  'ok' if rs.sort == ['g1', 'g2']
}

assert_equal 'ok', %q{
  def test n
    gs = (1..n).map do |i|
      Guild.new(i) do |i|
        "g#{i}"
      end
    end
    rs = []
    all_gs = gs.dup

    n.times{
      g, obj = Guild.select(*gs)
      rs << [g, obj]
      gs.delete(g)
    }
    rs = rs.sort_by{|g, obj| obj}
    if rs.map{|g,o| g} == all_gs &&
       rs.map{|g,o| o} == (1..n).map{|i| "g#{i}"}
      'ok'
    else
      'ng'
    end
  end

  30.times{|i|
    test i
  }
  'ok'
}

# communication channels belong to a guild will be closed
# if the guild is terminated.
assert_equal 'ok', %q{
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
}

assert_equal 'ok', %q{
  g = Guild.new do
  end

  g.recv # closed

  begin
    g.send(1)
  rescue Guild::Channel::ClosedError
    'ok'
  else
    'ng'
  end
}

# multiple guilds can recv (wait) from one channel
assert_equal '[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]', %q{
  ch = Guild::Channel.new
  GN = 10
  gs = GN.times.map{|i|
    Guild.new ch, i do |ch, i|
      msg = ch.recv
      msg # ping-pong
    end
  }
  GN.times{|i|
    ch << i
  }
  rs = GN.times.map{
    g, n = Guild.select(*gs)
    gs.delete g
    n
  }.sort
  rs
}

# multiple guilds can send to one channel
assert_equal '[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]', %q{
  ch = Guild::Channel.new
  GN = 10
  gs = GN.times.map{|i|
    Guild.new ch, i do |ch, i|
      ch << i
    end
  }
  rs = GN.times.map{
    ch.recv
  }.sort
  rs
}

assert_equal 'ok', %q{
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
  task_ch.close_send

  gn_results = (1..GN).map{
    g, obj = Guild.select(*gs)
    gs.delete(g)
    obj
  }

  'ok' if tn_results.size == TN &&
          gn_results.size == GN
}

# an exception in a guild will be re-raised at Guild#recv
assert_equal '[RuntimeError, "ok", true]', %q{
  g = Guild.new do
    raise 'ok' # exception will be transferred receiver
  end
  begin
    g.recv
  rescue Guild::RemoteError => e
    [e.cause.class,   #=> RuntimeError
     e.cause.message, #=> 'ok'
     e.guild == g]    #=> true
  end
}

# unshareable object are copied
assert_equal 'false', %q{
  obj = 'str'.dup
  g = Guild.new obj do |msg|
    msg.object_id
  end
  
  obj.object_id == g.recv
}

# To copy the object, now Marshal#dump is used
assert_equal 'no _dump_data is defined for class Thread', %q{
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
}

# send sharable and unsharable objects
assert_equal "[[[1, true], [:sym, true], [:xyzzy, true], [\"frozen\", true], " \
             "[(3/1), true], [(3+4i), true], [/regexp/, true], [C, true]], " \
             "[[\"mutable str\", false], [[:array], false], [{:hash=>true}, false]]]", %q{
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

  ur = unsharable_objects = ['mutable str'.dup, [:array], {hash: true}].map{|o|
    g << o
    o2 = g.recv
    [o, o.object_id == o2.object_id]
  }
  [sr, ur].inspect
}

# move example2: String
# touching moved object causes an error
assert_equal 'hello world', %q{
  # move
  g = Guild.new do
    obj = Guild.recv
    obj << ' world'
  end

  str = 'hello'
  g.move str
  modified = g.recv

  begin
    str << ' exception' # raise Guild::Channel::Error
  rescue Guild::Channel::Error
    modified #=> 'hello world'
  else
    raise 'unreachable'
  end
}

# move example2: Array
assert_equal '[0, 1]', %q{
  g = Guild.new do
    ary = Guild.recv
    ary << 1
  end

  a1 = [0]
  g.move a1
  a2 = g.recv
  begin
    a1 << 2 # raise Guild::Channel::Error
  rescue Guild::Channel::Error
    a2.inspect
  end
}

# Access to global-variables are prohibitted
assert_equal 'can not access global variables from non-main guild.', %q{
  $gv = 1
  g = Guild.new do
    $gv
  end

  begin
    g.recv
  rescue Guild::RemoteError => e
    e.cause.message
  end
}

# Access to global-variables are prohibitted
assert_equal 'can not access global variables from non-main guild.', %q{
  g = Guild.new do
    $gv = 1
  end

  begin
    g.recv
  rescue Guild::RemoteError => e
    e.cause.message
  end
}

# selfs are different objects
assert_equal 'false', %q{
  g = Guild.new do
    self.object_id
  end
  g.recv == self.object_id
}

# self is a guild instance
assert_equal 'true', %q{
  g = Guild.new do
    self.object_id
  end
  g.object_id == g.recv
}

# given block Proc will be isolated, so can not access outer variables.
assert_equal 'ArgumentError', %q{
  begin
    a = true
    g = Guild.new do
      a
    end
  rescue => e
    e.class
  end
}

# ivar in sharable-objects are not allowed to access from non-main guild
assert_equal 'can not access instance variables of classes/modules from non-main guild.', %q{
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
  rescue Guild::RemoteError => e
    e.cause.message
  end
}

# ivar in sharable-objects are not allowed to access from non-main guild
assert_equal 'can not access instance variables of shareable objects from non-main guild.', %q{
  ch = Guild::Channel.new
  ch.instance_variable_set(:@iv, 'str')

  g = Guild.new ch do |ch|
    p ch.instance_variable_get(:@iv)
  end

  begin
    g.recv
  rescue Guild::RemoteError => e
    e.cause.message
  end
}

# cvar in sharable-objects are not allowed to access from non-main guild
assert_equal 'can not access class variables from non-main guild.', %q{
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
  rescue Guild::RemoteError => e
    e.cause.message
  end
}

# Getting non-sharable objects via constants by other guilds is not allowed
assert_equal 'can not access non-sharable objects in constant CONST by non-main guild.', %q{
  class C
    CONST = 'str'
  end
  g = Guild.new do
    C::CONST
  end
  begin
    g.recv
  rescue Guild::RemoteError => e
    e.cause.message
  end
}

# Setting non-sharable objects into constants by other guilds is not allowed
assert_equal 'can not set constants with non-shareable objects by non-main guilds', %q{
  class C
  end
  g = Guild.new do
    C::CONST = 'str'
  end
  begin
    g.recv
  rescue Guild::RemoteError => e
    e.cause.message
  end
}

# A guild can have a name
assert_equal 'test-name', %q{
  g = Guild.new name: 'test-name' do
  end
  g.name
}

# If Guild doesn't have a name, Guild#name returns nil.
assert_equal 'nil', %q{
  g = Guild.new do
  end
  g.name.inspect
}
