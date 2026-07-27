# gen4 shared-config: routing/validation RULES as shareable lambdas in a
# frozen table; workers select and invoke rules per item; per-worker
# accumulators stay unshareable.
# axes: transfer=copy(results), shared shareable_lambda table, GC=periodic GC.start
RULES = Ractor.make_shareable(
  {
    double: Ractor.shareable_lambda { |x| x * 2 },
    square: Ractor.shareable_lambda { |x| x * x },
    clamp:  Ractor.shareable_lambda { |x| x > 50 ? 50 : x },
    tag:    Ractor.shareable_lambda { |x| "v#{x}".size },
  }
)
ORDER = Ractor.make_shareable(%i[double square clamp tag])

N_WORKERS = 4
ITEMS = 300

workers = N_WORKERS.times.map do |wid|
  Ractor.new(wid, ITEMS) do |id, n|
    acc = 0
    n.times do |i|
      rule = RULES[ORDER[(id + i) % ORDER.size]]
      acc += rule.call((i + id) % 90)
      GC.start if i % 100 == 99
    end
    acc
  end
end

got = workers.sum(&:value)
exp = 0
N_WORKERS.times do |id|
  ITEMS.times do |i|
    exp += RULES[ORDER[(id + i) % ORDER.size]].call((i + id) % 90)
  end
end
raise "FAIL #{got} != #{exp}" unless got == exp
puts "OK cfg_shareable_lambda_rules"
