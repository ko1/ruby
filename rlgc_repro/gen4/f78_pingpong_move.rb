# f78 ping-pong: one workpiece moved main->worker->main repeatedly, growing a travel log
# axes: move both directions (Ractor#send + Port#send move:), GC.start each hop
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
    mm[:log] << :worker
    mm[:hops] += 1
    po.send(mm, move: true)
  end
end

ball = { hops: 0, log: [], payload: +"resilient" }
rounds = STRESS ? 3 : 8
rounds.times do |i|
  w.send(ball, move: true)
  begin
    ball[:hops]
    raise "ball not husked after serve #{i}"
  rescue Ractor::MovedError
  end
  ball = port.receive # moved back: fresh reference, same structure
  ball[:log] << :main
  GC.start if i.odd?
end
assert ball[:hops] == rounds, "hops #{ball[:hops]}"
assert ball[:log] == [:worker, :main] * rounds, "alternating log"
assert ball[:payload] == "resilient", "payload survived #{rounds} moves"
ball[:payload] << "!" # still mutable after all those moves
assert ball[:payload] == "resilient!", "mutability retained"
w.send(:eof)
puts "OK f78_pingpong_move"
