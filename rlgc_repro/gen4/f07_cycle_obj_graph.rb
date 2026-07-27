# f07 peer-mesh app: plain objects with mutual ivar cycles moved to worker
# axes: move, ivar cycles, GC.start both sides
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

class Node
  attr_accessor :name, :peer, :buddy
  def initialize(name)
    @name = name
  end
end

port = Ractor::Port.new
w = Ractor.new(port) do |po|
  mm = Ractor.receive
  GC.start
  aa, bb = mm
  po.send([aa.peer.equal?(bb), bb.peer.equal?(aa), aa.buddy.equal?(aa), aa.name, bb.name])
end

x = Node.new("alpha")
y = Node.new("beta")
x.peer = y
y.peer = x
x.buddy = x # self-cycle through ivar
w.send([x, y], move: true)
begin
  x.name
  raise "obj source not husked"
rescue Ractor::MovedError
end
GC.start
p1, p2, p3, n1, n2 = port.receive
assert p1 && p2, "mutual ivar cycle broken"
assert p3, "self ivar cycle broken"
assert n1 == "alpha" && n2 == "beta", "names lost"
puts "OK f07_cycle_obj_graph"
