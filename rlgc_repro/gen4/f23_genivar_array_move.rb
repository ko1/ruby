# f23 batch mover: generic ivars on Arrays, moved to consumer, ivars travel with move
# axes: move, generic ivars on Array, husk assert, GC.start in worker
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
    GC.start
    po.send([mm.sum, mm.instance_variable_get(:@origin), mm.instance_variable_get(:@rev)])
  end
end

rounds = STRESS ? 2 : 5
rounds.times do |i|
  arr = [i, i + 1, i + 2, i + 3]
  arr.instance_variable_set(:@origin, "node-#{i}")
  arr.instance_variable_set(:@rev, i * 11)
  w.send(arr, move: true)
  begin
    arr.sum
    raise "array not husked"
  rescue Ractor::MovedError
  end
  sum, origin, rev = port.receive
  assert sum == 4 * i + 6, "sum round #{i}"
  assert origin == "node-#{i}", "moved ivar origin round #{i}"
  assert rev == i * 11, "moved ivar rev round #{i}"
end
w.send(:eof)
puts "OK f23_genivar_array_move"
