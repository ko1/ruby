# f05 token-ring app: cyclic Array ring moved to worker; husk assert on source
# axes: move with cycles, Ractor::MovedError, GC.start after move
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

port = Ractor::Port.new
w = Ractor.new(port) do |po|
  loop do
    mm = Ractor.receive
    break if mm == :eof
    # ring: each node = [payload, next_node]; verify closure in 4 hops
    cur = mm
    names = []
    4.times { names << cur[0]; cur = cur[1] }
    po.send([cur.equal?(mm), names])
  end
end

rounds = STRESS ? 2 : 4
rounds.times do |i|
  n1 = ["a#{i}", nil]; n2 = ["b#{i}", nil]; n3 = ["c#{i}", nil]; n4 = ["d#{i}", nil]
  n1[1] = n2; n2[1] = n3; n3[1] = n4; n4[1] = n1
  w.send(n1, move: true)
  begin
    n1[0]
    raise "ring source not husked"
  rescue Ractor::MovedError
  end
  GC.start
  closed, names = port.receive
  assert closed, "round #{i}: ring not closed after move"
  assert names == ["a#{i}", "b#{i}", "c#{i}", "d#{i}"], "round #{i}: order #{names.inspect}"
end
w.send(:eof)
puts "OK f05_cycle_ring_move"
