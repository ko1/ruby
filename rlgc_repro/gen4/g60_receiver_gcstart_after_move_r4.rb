# 4 sinks GC.start right after receiving a moved batch, then read it (post-materialization read)
# axes: 4 ractors, move send, GC.start inside receiver, gc right after receive
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
NR = 4
M = 6
ports = NR.times.map { Ractor::Port.new }
rs = NR.times.map do |i|
  Ractor.new(ports[i], i) do |po, id|
    acc = 0
    n = 0
    loop do
      msg = Ractor.receive
      break if msg == :stop
      GC.start if n == 0
      n += 1
      acc += msg.sum(&:bytesize)
    end
    po.send(acc)
  end
end
exp = 0
M.times do |k|
  batch = Array.new(8) { |i| +"rc#{k}-#{i}" }
  exp += batch.sum(&:bytesize)
  rs[k % NR].send(batch, move: true)
end
rs.each { |r| r.send(:stop) }
got = ports.sum { |p| p.receive }
rs.each(&:value)
raise "got=#{got} exp=#{exp}" unless got == exp
puts "OK g60_receiver_gcstart_after_move_r4"
