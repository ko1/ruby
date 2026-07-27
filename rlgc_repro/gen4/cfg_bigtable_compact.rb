# gen4 shared-config: LARGE shareable lookup table; main runs GC.compact
# repeatedly while workers continuously read table entries (shareable objects
# must stay pinned/valid across local compactions).
# axes: transfer=copy(results), GC=GC.compact loop in main + GC.start in workers
TABLE = Ractor.make_shareable(
  Array.new(3000) { |i| ["entry-#{i}".freeze, [i, i * 2, "meta#{i % 13}"]] }
)

N_WORKERS = 4
ROUNDS = 5

ctl = Ractor::Port.new
workers = N_WORKERS.times.map do |wid|
  Ractor.new(ctl, wid, ROUNDS) do |c, id, rounds|
    total = 0
    rounds.times do |r|
      sum = 0
      step = id + 1
      (0...TABLE.size).step(step) do |i|
        name, (a, b, meta) = TABLE[i]
        sum += a + b + name.size + meta.size
      end
      GC.start if r == 2
      c << [id, r, sum]
      total += sum
    end
    total
  end
end

# main compacts while collecting round reports
per_round_exp = N_WORKERS.times.map do |id|
  step = id + 1
  (0...TABLE.size).step(step).sum do |i|
    name, (a, b, meta) = TABLE[i]
    a + b + name.size + meta.size
  end
end

reports = 0
grand = 0
(N_WORKERS * ROUNDS).times do
  id, _r, sum = ctl.receive
  raise "FAIL round sum w#{id}" unless sum == per_round_exp[id]
  reports += 1
  grand += sum
  GC.compact if reports % 4 == 0
end
raise "FAIL totals" unless workers.sum(&:value) == grand
puts "OK cfg_bigtable_compact"
