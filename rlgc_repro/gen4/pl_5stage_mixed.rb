# gen4 pipeline: 5-stage transform with a different payload shape at each
# stage boundary (string -> array -> hash -> struct-like array -> int).
# axes: transfer=copy, GC=none, exceptions=none, payload=mixed shapes
N_ITEMS = 400
Rec = Struct.new(:id, :total, :label)

out = Ractor::Port.new

s5 = Ractor.new(out) do |o|
  n = acc = 0
  while (m = Ractor.receive) != :eos
    n += 1
    acc += m
  end
  o << [n, acc]
end

s4 = Ractor.new(s5) do |nxt|  # Rec -> int
  while (m = Ractor.receive) != :eos
    nxt << m.total + m.label.size
  end
  nxt << :eos
end

s3 = Ractor.new(s4) do |nxt|  # hash -> Rec
  while (m = Ractor.receive) != :eos
    nxt << Rec.new(m[:id], m[:vals].sum, "rec-#{m[:id] % 5}")
  end
  nxt << :eos
end

s2 = Ractor.new(s3) do |nxt|  # array -> hash
  while (m = Ractor.receive) != :eos
    nxt << { id: m[0], vals: m[1..] }
  end
  nxt << :eos
end

s1 = Ractor.new(s2) do |nxt|  # string -> array of ints
  while (m = Ractor.receive) != :eos
    nxt << m.split(",").map(&:to_i)
  end
  nxt << :eos
end

expected = 0
N_ITEMS.times do |i|
  vals = [i % 7, i % 13, 5]
  expected += vals.sum + "rec-#{i % 5}".size
  s1 << ([i] + vals).join(",")
end
s1 << :eos

n, acc = out.receive
[s1, s2, s3, s4, s5].each(&:join)
raise "FAIL n #{n}" unless n == N_ITEMS
raise "FAIL acc #{acc} != #{expected}" unless acc == expected
puts "OK pl_5stage_mixed"
