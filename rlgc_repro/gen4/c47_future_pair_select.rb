# c47: two competing futures on separate ports; main receives both and commits
# to the deterministic min; frozen shareable input table; final GC.compact.
# All ports are pre-created before enabling stress (>=4 Port.new under GC.stress
# self-deadlocks: port-table growth allocs under ractor_lock vs GC mark).
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false

PAIRS = STRESS ? 3 : 6
INPUTS = Ractor.make_shareable(Array.new(2 * PAIRS) { |i| (i * 37) % 11 + 3 })

port_pairs = PAIRS.times.map { [Ractor::Port.new, Ractor::Port.new] }
GC.stress = true if STRESS

results = []
rs = []
PAIRS.times do |k|
  fa, fb = port_pairs[k]
  ra = Ractor.new(fa, 2 * k) { |f, idx| f << [:res, idx, INPUTS[idx] * 100]; :done }
  rb = Ractor.new(fb, 2 * k + 1) { |f, idx| f << [:res, idx, INPUTS[idx] * 100]; :done }
  ta, ia, va = fa.receive
  tb, ib, vb = fb.receive
  raise "tags" unless ta == :res && tb == :res && ia == 2 * k && ib == 2 * k + 1
  raise "vals" unless va == INPUTS[ia] * 100 && vb == INPUTS[ib] * 100
  results << [va, vb].min
  rs << ra << rb
end
expected = PAIRS.times.map { |k| [INPUTS[2 * k] * 100, INPUTS[2 * k + 1] * 100].min }
raise "mins" unless results == expected
GC.stress = false
rs.each { |r| raise unless r.value == :done }
GC.compact
puts "OK c47_future_pair_select"
