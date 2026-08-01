# 総合 mix: CSV parse (copy) -> key group-by (disjoint 2 workers) -> top-2 部分結果を move 回収
# -> shareable 表で名前解決、全段厳密検証
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

NAMES = Ractor.make_shareable((0...6).to_h { |k| [k, "grp#{k}"] })
N = 36
rows = Array.new(N) { |i| "#{i % 6},#{(i * 29) % 83}" }
exp_tot = Hash.new(0)
N.times { |i| exp_tot[i % 6] += (i * 29) % 83 }
exp_top = exp_tot.sort_by { |k, v| [-v, k] }.first(2).map { |k, v| [NAMES[k], v] }

out = Ractor::Port.new
ws = 2.times.map do
  Ractor.new(out) do |o|
    h = Hash.new(0)
    loop do
      row = Ractor.receive
      break if row == :stop
      k, v = row.split(",").map { |x| Integer(x) }
      h[k] += v
    end
    o.send(h, move: true)
  end
end
N.times { |i| ws[(i % 6) % 2].send(rows[i]) }
ws.each { |w| w.send(:stop) }
merged = {}
2.times do
  out.receive.each do |k, v|
    raise "dup #{k}" if merged.key?(k)
    merged[k] = v
  end
end
ws.each(&:value)
GC.compact
raise "totals" unless merged == exp_tot
top = merged.sort_by { |k, v| [-v, k] }.first(2).map { |k, v| [NAMES[k], v] }
raise "top=#{top} exp=#{exp_top}" unless top == exp_top
puts "OK b80_grand_mix_etl"
