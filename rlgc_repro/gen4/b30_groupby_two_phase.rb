# 2-phase group-by: 2 mapper が部分集計 -> main が key range で 2 reducer へ shuffle
# axes: 4 workers (2+2), copy, shuffle via main
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 40
NKEYS = 10
exp = Hash.new(0)
N.times { |i| exp[(i * 7) % NKEYS] += i }

mp = Ractor::Port.new
mappers = 2.times.map do |mi|
  Ractor.new(mp, mi, N, NKEYS) do |o, id, n, nk|
    h = Hash.new(0)
    (id...n).step(2) { |i| h[(i * 7) % nk] += i }
    o.send(h)
  end
end
partials = 2.times.map { mp.receive }
mappers.each(&:value)
reducers = 2.times.map do |ri|
  Ractor.new(ri, NKEYS) do |id, nk|
    h = Hash.new(0)
    2.times do
      part = Ractor.receive
      part.each { |k, v| h[k] += v }
    end
    h
  end
end
partials.each do |part|
  lo = part.select { |k, _| k < NKEYS / 2 }
  hi = part.select { |k, _| k >= NKEYS / 2 }
  reducers[0].send(lo)
  reducers[1].send(hi)
end
merged = {}
reducers.each do |r|
  r.value.each do |k, v|
    raise "dup #{k}" if merged.key?(k)
    merged[k] = v
  end
end
raise "merged=#{merged}" unless merged == exp
puts "OK b30_groupby_two_phase"
