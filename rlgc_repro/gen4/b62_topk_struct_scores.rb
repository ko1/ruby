# Struct score レコードの top-4 を 2 worker + main merge で求める
# axes: 2 workers, Struct payload, copy, deterministic tie-break
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

Sc = Struct.new(:id, :pts)
N = 36
recs = Array.new(N) { |i| Sc.new(i, (i * 67) % 90) }
exp = recs.sort_by { |r| [-r.pts, r.id] }.first(4).map { |r| [r.id, r.pts] }

out = Ractor::Port.new
ws = 2.times.map do |wi|
  Ractor.new(out, recs.select { |r| r.id % 2 == wi }) do |o, part|
    o.send(part.sort_by { |r| [-r.pts, r.id] }.first(4))
  end
end
cands = []
2.times { cands.concat(out.receive) }
ws.each(&:value)
GC.start
got = cands.sort_by { |r| [-r.pts, r.id] }.first(4).map { |r| [r.id, r.pts] }
raise "top=#{got} exp=#{exp}" unless got == exp
puts "OK b62_topk_struct_scores"
