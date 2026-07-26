# 1 workers concurrently GC.start while main streams copied batches (concurrent GC during receive)
# axes: 1 ractors, copy send, GC.start concurrent in workers, gc during receive stream
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
NR = 1
M = 12
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
      acc += msg[:data].sum(&:bytesize)
    end
    po.send(acc)
  end
end
exp = 0
M.times do |k|
  batch = Array.new(6) { |i| +"cg#{k}-#{i}" }
  exp += batch.sum(&:bytesize)
  rs[k % NR].send({ data: batch })
end
rs.each { |r| r.send(:stop) }
got = ports.sum { |p| p.receive }
rs.each(&:value)
raise "got=#{got} exp=#{exp}" unless got == exp
puts "OK g61_concurrent_gcstart_workers_r1"
