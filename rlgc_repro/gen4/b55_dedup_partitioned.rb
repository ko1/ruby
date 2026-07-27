# 重複だらけの id stream を id%NW で partition し unique 数を厳密検証
# axes: 4 workers, copy, Hash による dedup
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 80
NW = 4
ids = Array.new(N) { |i| (i * i + i) % 25 }
exp_uniq = ids.uniq.size

out = Ractor::Port.new
ws = NW.times.map do
  Ractor.new(out) do |o|
    seen = {}
    loop do
      id = Ractor.receive
      break if id == :stop
      seen[id] = true
    end
    o.send(seen.size)
  end
end
ids.each { |id| ws[id % NW].send(id) }
ws.each { |w| w.send(:stop) }
got = 0
NW.times { got += out.receive }
ws.each(&:value)
raise "uniq=#{got} exp=#{exp_uniq}" unless got == exp_uniq
puts "OK b55_dedup_partitioned"
