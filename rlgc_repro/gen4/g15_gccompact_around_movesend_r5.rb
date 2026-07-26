# main moves batches to 5 sinks, driving GC.compact before and after a move (materialization window)
# axes: 5 ractors, move send, GC.compact, gc around bounded set of moves
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
NR = 5
M = 5
ports = NR.times.map { Ractor::Port.new }
rs = NR.times.map do |i|
  Ractor.new(ports[i], i) do |po, id|
    acc = 0
    loop do
      msg = Ractor.receive
      break if msg == :stop
      acc += msg.sum(&:bytesize)
    end
    po.send(acc)
  end
end
exp = 0
M.times do |k|
  batch = Array.new(8) { |i| +"mv#{k}-#{i}" }
  exp += batch.sum(&:bytesize)
  GC.compact if k % 5 == 0
  rs[k % NR].send(batch, move: true)
  GC.compact if k % 5 == 0
end
rs.each { |r| r.send(:stop) }
got = ports.sum { |p| p.receive }
rs.each(&:value)
raise "got=#{got} exp=#{exp}" unless got == exp
puts "OK g15_gccompact_around_movesend_r5"
