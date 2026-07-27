# worker 側でも GC.stress を立てる小型 ETL (段境界で GC.start)
# axes: 2 workers, worker-side stress, tiny payloads
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 12
out = Ractor::Port.new
ws = 2.times.map do |wid|
  Ractor.new(out, wid) do |o, id|
    GC.stress = true if ENV['S_STRESS']
    loop do
      job = Ractor.receive
      break if job == :stop
      o.send([job[0], job[1].chars.map(&:ord).sum])
    end
    GC.stress = false
  end
end
expected = {}
N.times do |k|
  s = "e#{k}-#{k * 3}"
  expected[k] = s.chars.map(&:ord).sum
  ws[k % 2].send([k, s])
end
GC.start
got = {}
N.times do
  k, v = out.receive
  got[k] = v
end
ws.each { |w| w.send(:stop) }
ws.each(&:value)
raise "got #{got.size}" unless got == expected
puts "OK b09_etl_worker_stress_gc"
