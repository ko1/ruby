# 不正行 (列不足) 混じりの CSV を worker が弾き、valid/skip 件数を厳密検証
# axes: 3 workers, copy, error-path counting
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 42
rows = Array.new(N) { |i| i % 7 == 3 ? "bad#{i}" : "#{i},x,#{i * 2}" }
exp_bad = (0...N).count { |i| i % 7 == 3 }
exp_sum = (0...N).sum { |i| i % 7 == 3 ? 0 : i * 2 }

out = Ractor::Port.new
ws = 3.times.map do
  Ractor.new(out) do |o|
    ok = 0
    bad = 0
    sum = 0
    loop do
      r = Ractor.receive
      break if r == :stop
      c = r.split(",")
      if c.size == 3
        ok += 1
        sum += Integer(c[2])
      else
        bad += 1
      end
    end
    o.send([ok, bad, sum])
  end
end
N.times { |i| ws[i % 3].send(rows[i]) }
ws.each { |w| w.send(:stop) }
ok = bad = sum = 0
3.times do
  a, b, s = out.receive
  ok += a
  bad += b
  sum += s
end
ws.each(&:value)
raise "bad=#{bad}" unless bad == exp_bad
raise "ok" unless ok == N - exp_bad
raise "sum=#{sum}" unless sum == exp_sum
puts "OK b25_csv_dirty_filter"
