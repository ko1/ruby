# 分散 top-5: key disjoint な 5 worker が local top-5 を出し main が merge (正確性は disjoint 性で保証)
# axes: 5 workers, copy, candidate merge
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 75
NW = 5
K = 5
pairs = Array.new(N) { |i| [i % 25, (i * 91) % 401] } # key, score
best = Hash.new(-1)
pairs.each { |k, s| best[k] = s if s > best[k] }
exp = best.sort_by { |k, s| [-s, k] }.first(K)

out = Ractor::Port.new
ws = NW.times.map do
  Ractor.new(out, K) do |o, kk|
    loc = Hash.new(-1)
    loop do
      msg = Ractor.receive
      break if msg == :stop
      k, s = msg
      loc[k] = s if s > loc[k]
    end
    o.send(loc.sort_by { |k, s| [-s, k] }.first(kk))
  end
end
pairs.each { |k, s| ws[k % NW].send([k, s]) }
ws.each { |w| w.send(:stop) }
cands = []
NW.times { cands.concat(out.receive) }
ws.each(&:value)
got = cands.sort_by { |k, s| [-s, k] }.first(K)
raise "top=#{got} exp=#{exp}" unless got == exp
puts "OK b60_topk_distributed"
