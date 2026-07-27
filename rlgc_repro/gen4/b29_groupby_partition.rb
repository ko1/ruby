# key を数値 hash-partition して 4 worker が disjoint に group-by、merge で衝突なしを検証
# axes: 4 workers, copy, key % NW routing
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 48
NW = 4
NKEYS = 12
exp = Hash.new(0)
N.times { |i| exp[i % NKEYS] += i * 3 }

out = Ractor::Port.new
ws = NW.times.map do
  Ractor.new(out) do |o|
    h = Hash.new(0)
    loop do
      msg = Ractor.receive
      break if msg == :stop
      k, v = msg
      h[k] += v
    end
    o.send(h)
  end
end
N.times do |i|
  k = i % NKEYS
  ws[k % NW].send([k, i * 3])
end
ws.each { |w| w.send(:stop) }
merged = {}
NW.times do
  out.receive.each do |k, v|
    raise "dup #{k}" if merged.key?(k)
    merged[k] = v
  end
end
ws.each(&:value)
raise "merged" unless merged == exp
puts "OK b29_groupby_partition"
