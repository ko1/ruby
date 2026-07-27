# 2-phase dedup: phase1 で候補 (2 回以上) を検出、phase2 respawn worker が正確な重複数を検証
# axes: 2+2 workers respawn, copy, worker 側 stress は phase2 のみ
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 40
keys = Array.new(N) { |i| (i * 3) % 16 }
counts = Hash.new(0)
keys.each { |k| counts[k] += 1 }
exp_dups = counts.count { |_, c| c >= 2 }

# phase1: 偶奇 partition で counts
p1 = Ractor::Port.new
w1 = 2.times.map do |wi|
  Ractor.new(p1, keys.select { |k| k % 2 == wi }) do |o, part|
    h = Hash.new(0)
    part.each { |k| h[k] += 1 }
    o.send(h)
  end
end
cand = {}
2.times { p1.receive.each { |k, c| cand[k] = c if c >= 2 } }
w1.each(&:value)
# phase2: 候補 key の件数を再検証 (respawn)
p2 = Ractor::Port.new
w2 = 2.times.map do |wi|
  Ractor.new(p2, keys, cand.keys.select { |k| k % 2 == wi }) do |o, all, ks|
    GC.stress = true if ENV['S_STRESS']
    n = ks.count { |k| all.count(k) >= 2 }
    GC.stress = false
    o.send(n)
  end
end
got = 0
2.times { got += p2.receive }
w2.each(&:value)
raise "dups=#{got} exp=#{exp_dups}" unless got == exp_dups
puts "OK b61_dedup_two_phase"
