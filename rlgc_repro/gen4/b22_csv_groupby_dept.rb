# CSV を dept で hash-partition し、各 worker が担当 dept のみ集計する group-by
# axes: 3 workers (disjoint keys), copy, per-dept exact totals
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

DEPTS = %w[eng ops sales hr fin].freeze
N = 36
rows = Array.new(N) { |i| "#{i},#{DEPTS[i % 5]},#{(i * 11) % 200}" }
exp = Hash.new(0)
N.times { |i| exp[DEPTS[i % 5]] += (i * 11) % 200 }

NW = 3
out = Ractor::Port.new
ws = NW.times.map do
  Ractor.new(out) do |o|
    h = Hash.new(0)
    loop do
      r = Ractor.receive
      break if r == :stop
      _, d, v = r.split(",")
      h[d] += Integer(v)
    end
    o.send(h)
  end
end
N.times { |i| ws[(i % 5) % NW].send(rows[i]) } # dept ごとに固定 worker (disjoint)
ws.each { |w| w.send(:stop) }
merged = {}
NW.times do
  out.receive.each do |k, v|
    raise "dup key #{k}" if merged.key?(k)
    merged[k] = v
  end
end
ws.each(&:value)
raise "merged=#{merged} exp=#{exp}" unless merged == exp
puts "OK b22_csv_groupby_dept"
