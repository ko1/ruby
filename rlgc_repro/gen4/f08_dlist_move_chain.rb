# f08 queue-audit app: circular doubly-linked list moved through a 3-ractor chain
# axes: move with cycles, chain lifecycle, GC.start per stage
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

class DNode
  attr_accessor :val, :nxt, :prv
  def initialize(val)
    @val = val
  end
end

def make_dlist(vals)
  nodes = vals.map { |v| DNode.new(v) }
  nodes.each_with_index do |nd, i|
    nd.nxt = nodes[(i + 1) % nodes.size]
    nd.prv = nodes[(i - 1) % nodes.size]
  end
  nodes.first
end

out = Ractor::Port.new
s3 = Ractor.new(out) do |po|
  mm = Ractor.receive
  GC.start
  fwd = []; cur = mm; 5.times { fwd << cur.val; cur = cur.nxt }
  bwd = []; cur = mm; 5.times { bwd << cur.val; cur = cur.prv }
  po.send([fwd, bwd, cur.equal?(mm)])
end
s2 = Ractor.new(s3) do |nxt|
  mm = Ractor.receive
  mm.val = mm.val * 10
  nxt.send(mm, move: true)
end
s1 = Ractor.new(s2) do |nxt|
  mm = Ractor.receive
  GC.start
  nxt.send(mm, move: true)
end

head = make_dlist([1, 2, 3, 4, 5])
s1.send(head, move: true)
begin
  head.val
  raise "dlist source not husked"
rescue Ractor::MovedError
end
fwd, bwd, closed = out.receive
assert fwd == [10, 2, 3, 4, 5], "forward walk #{fwd.inspect}"
assert bwd == [10, 5, 4, 3, 2], "backward walk #{bwd.inspect}"
assert closed, "dlist ring not closed"
puts "OK f08_dlist_move_chain"
