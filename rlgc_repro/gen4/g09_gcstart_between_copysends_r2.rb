# 2 readers accumulate copied batches; main forces GC.start between sends (mid-send adversarial window)
# axes: 2 ractors, copy send, GC.start, gc at bounded send boundaries
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
NR = 2
M = 12
ports = NR.times.map { Ractor::Port.new }
rs = NR.times.map do |i|
  Ractor.new(ports[i], i) do |po, id|
    acc = 0
    loop do
      msg = Ractor.receive
      break if msg == :stop
      acc += msg[:data].sum(&:bytesize)
    end
    po.send(acc)
  end
end
exp = 0
M.times do |k|
  batch = Array.new(6) { |i| +"a#{k}-#{i}" }
  exp += batch.sum(&:bytesize)
  rs[k % NR].send({ data: batch })
  GC.start if k % 5 == 0
end
rs.each { |r| r.send(:stop) }
got = ports.sum { |p| p.receive }
rs.each(&:value)
raise "got=#{got} exp=#{exp}" unless got == exp
puts "OK g09_gcstart_between_copysends_r2"
