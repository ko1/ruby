# frozen shareable な log corpus を参照渡しし、worker は index range だけ受け取って読む
# axes: 4 workers, make_shareable reference, range jobs, GC.compact after share
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 30
CORPUS = Ractor.make_shareable(Array.new(N) { |i| "lvl=#{i % 5} size=#{(i * 13) % 97}" })
GC.compact
expected = (0...N).sum { |i| (i * 13) % 97 } # corpus を parse せず算術で

out = Ractor::Port.new
NW = 4
ws = NW.times.map do
  Ractor.new(out, CORPUS) do |o, corpus|
    loop do
      rng = Ractor.receive
      break if rng == :stop
      o.send(rng.sum { |i| corpus[i][/size=(\d+)/, 1].to_i })
    end
  end
end
jobs = 0
(0...N).each_slice(6) do |sl|
  ws[jobs % NW].send(sl.first..sl.last)
  jobs += 1
end
got = 0
jobs.times { got += out.receive }
ws.each { |w| w.send(:stop) }
ws.each(&:value)
raise "got=#{got} exp=#{expected}" unless got == expected
puts "OK b16_log_shareable_corpus"
