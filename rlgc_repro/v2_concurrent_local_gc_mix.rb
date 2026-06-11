# M1b adversarial mix: 12 workers run concurrent lock-free local GCs
# (minor + staggered majors that escalate to global cycles) against
# generic-ivar churn, while diers terminate and get absorbed through
# Ractor#value and main churns + GCs concurrently.
#
# Caught two M1b races in ractor_mark (a Ractor object's dmark walked a
# LIVE foreign Ractor's owner-mutated state):
# - threads/ECs/fibers: freed concurrently by the dying owner ->
#   rb_execution_context_mark UAF ("Segmentation fault" in mark).
# - sync queues / port table / monitors (and hook/storage tables):
#   resized in place by the running owner -> torn reads marked junk
#   ("[BUG] try to mark T_NONE object (obj: out-of-heap...)").
# Fixed by walking owner-mutated structures only when no concurrent
# owner can exist (own Ractor / terminated / global GC's barrier);
# single stable slots (default port, stdio) stay always-marked.
DURATION = (ARGV[0] || 30).to_i
stop_at = Process.clock_gettime(Process::CLOCK_MONOTONIC) + DURATION

workers = 12.times.map do |wi|
  Ractor.new(wi, stop_at) do |wi, stop_at|
    ring = []
    k = 0
    while Process.clock_gettime(Process::CLOCK_MONOTONIC) < stop_at
      k += 1
      s = "w#{wi}-#{k}"
      s.instance_variable_set(:@a, [k, "v#{k}"])
      ring << s << [k, k.to_s] << {k => s}
      ring.shift(3) if ring.size > 90
      GC.start(full_mark: false) if k % 4_000 == 0
      GC.start if k % 40_000 == (wi * 1000) % 40_000  # staggered fulls -> global GC
    end
    k
  end
end

# churner ractors that die unjoined while others run
diers = []
spawn_count = 0
main_ring = []
mk = 0
while Process.clock_gettime(Process::CLOCK_MONOTONIC) < stop_at
  mk += 1
  if mk % 2_000 == 0 && spawn_count < 60
    spawn_count += 1
    diers << Ractor.new(mk) { |m| a = (1..500).map { |i| "d#{m}-#{i}" }; a.size }
    if diers.size > 6
      r = diers.shift
      r.value # absorb
    end
  end
  s = +"m#{mk}"
  s.instance_variable_set(:@m, mk)
  main_ring << s
  main_ring.shift if main_ring.size > 120
  GC.start(full_mark: false) if mk % 6_000 == 0
end

results = workers.map(&:value)
diers.each(&:value)
raise "worker died early" if results.any? { |r| r < 1000 }
puts "M1B_MIX_OK iters=#{results.sum + mk}"
