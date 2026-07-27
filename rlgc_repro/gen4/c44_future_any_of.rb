# c44: any-of: N racers resolve one shared port; first arrival is the winner
# (must be a valid candidate); the rest are drained and the full set asserted.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

N = STRESS ? 4 : 7

race = Ractor::Port.new
racers = N.times.map do |i|
  Ractor.new(race, i) do |rp, id|
    rp << [:candidate, id, id * id + 1]
    :raced
  end
end

tag, wid, wval = race.receive     # winner: nondeterministic id, deterministic form
raise "wtag" unless tag == :candidate
raise "wid range" unless (0...N).cover?(wid)
raise "wval" unless wval == wid * wid + 1
rest = (N - 1).times.map do
  t, id, v = race.receive
  raise "rtag" unless t == :candidate && v == id * id + 1
  id
end
raise "set" unless (rest + [wid]).sort == (0...N).to_a
GC.stress = false
racers.each { |r| raise unless r.value == :raced }
GC.start
puts "OK c44_future_any_of"
