# 幅広 CSV (30 列) を少数行だけ処理: 列 7 と列 23 の合計を検証 (payload size 軸)
# axes: 2 workers, copy, wide rows few records
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

NR = 10
NCOL = 30
rows = Array.new(NR) { |r| Array.new(NCOL) { |c| (r * 31 + c * 7) % 100 }.join(",") }
exp7 = (0...NR).sum { |r| (r * 31 + 7 * 7) % 100 }
exp23 = (0...NR).sum { |r| (r * 31 + 23 * 7) % 100 }

out = Ractor::Port.new
ws = 2.times.map do
  Ractor.new(out) do |o|
    s7 = 0
    s23 = 0
    loop do
      row = Ractor.receive
      break if row == :stop
      c = row.split(",")
      s7 += Integer(c[7])
      s23 += Integer(c[23])
    end
    o.send([s7, s23])
  end
end
NR.times { |r| ws[r % 2].send(rows[r]) }
ws.each { |w| w.send(:stop) }
g7 = g23 = 0
2.times do
  a, b = out.receive
  g7 += a
  g23 += b
end
ws.each(&:value)
raise "col7 #{g7} != #{exp7}" unless g7 == exp7
raise "col23 #{g23} != #{exp23}" unless g23 == exp23
puts "OK b27_csv_wide_rows"
