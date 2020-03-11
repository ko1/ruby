assert_equal 'ok', %q{
  # join
  g = Guild.new do
    'ok'
  end
  g.recv
}

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

assert_equal 'ok', %q{
  # ping-pong with channel
  g = Guild.new do
    msg = Guild.recv
  end
  g.send 'ok'
  g.recv
}

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

assert_equal 'ok', %q{
  g = Guild.new do
    raise 'ok' # exception will be transferred receiver
  end
  begin
    g.recv
  rescue => e
    e.message
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

# global-variable $gv
assert_equal '[0, 1]', %q{
  $gv = 1
  g = Guild.new do
    $gv = 0 # $g is guild local variable.
    $gv
  end
  [g.recv, $gv]
}

# selfs are different objects
assert_equal 'false', %q{
  g = Guild.new do
    self.object_id
  end
  g.recv == self.object_id
}

# we can specify self class with self_class keyword.
assert_equal 'C', %q{
  class C; end
  g = Guild.new self_class: C do
    self.class
  end
  g.recv
}
