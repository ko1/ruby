# frozen Struct を Hash キーにした group-by (Struct の値等価 hash を跨 Ractor で利用)
# axes: 2 workers, copy, Struct key
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

GKey = Struct.new(:region, :tier)
N = 32
exp = Hash.new(0)
N.times { |i| exp[GKey.new(i % 3, i % 2).freeze] += i }

out = Ractor::Port.new
ws = 2.times.map do
  Ractor.new(out) do |o|
    h = Hash.new(0)
    loop do
      msg = Ractor.receive
      break if msg == :stop
      key, v = msg
      h[key] += v
    end
    o.send(h)
  end
end
N.times do |i|
  key = GKey.new(i % 3, i % 2).freeze
  ws[(i % 3) % 2].send([key, i]) # region で partition (disjoint)
end
ws.each { |w| w.send(:stop) }
merged = {}
2.times do
  out.receive.each do |k, v|
    raise "dup #{k}" if merged.key?(k)
    merged[k] = v
  end
end
ws.each(&:value)
raise "merged size" unless merged.size == exp.size
exp.each { |k, v| raise "key #{k}" unless merged[k] == v }
puts "OK b35_groupby_struct_key"
