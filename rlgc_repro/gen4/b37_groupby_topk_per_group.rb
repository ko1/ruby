# group ごとの top-2 値を group-owner worker が保持し main で厳密比較
# axes: 3 workers (group disjoint), copy, sorted-insert top-k
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

NG = 6
N = 48
vals = Array.new(N) { |i| [(i * 5) % NG, (i * i * 3) % 101] }
exp = Hash.new { |h, k| h[k] = [] }
vals.each { |g, v| exp[g] << v }
exp = exp.to_h { |g, vs| [g, vs.sort.reverse.first(2)] }

out = Ractor::Port.new
ws = 3.times.map do
  Ractor.new(out) do |o|
    top = Hash.new { |h, k| h[k] = [] }
    loop do
      msg = Ractor.receive
      break if msg == :stop
      g, v = msg
      a = top[g]
      a << v
      a.sort!.reverse!
      a.pop if a.size > 2
    end
    o.send(top.to_h { |g, a| [g, a] })
  end
end
vals.each { |g, v| ws[g % 3].send([g, v]) }
ws.each { |w| w.send(:stop) }
merged = {}
3.times do
  out.receive.each do |g, a|
    raise "dup #{g}" if merged.key?(g)
    merged[g] = a
  end
end
ws.each(&:value)
raise "merged=#{merged} exp=#{exp}" unless merged == exp
puts "OK b37_groupby_topk_per_group"
