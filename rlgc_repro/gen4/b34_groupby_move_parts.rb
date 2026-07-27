# partition 済み配列を move で渡し、worker の部分集計 Hash も move で回収
# axes: 4 workers, move both ways, per-key exact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

NW = 4
N = 40
exp = Hash.new(0)
N.times { |i| exp[i % 8] += i + 1 }

out = Ractor::Port.new
ws = NW.times.map do
  Ractor.new(out) do |o|
    part = Ractor.receive
    h = Hash.new(0)
    part.each { |k, v| h[k] += v }
    o.send(h, move: true)
  end
end
parts = Array.new(NW) { [] }
N.times do |i|
  k = i % 8
  parts[k % NW] << [k, i + 1]
end
NW.times { |w| ws[w].send(parts[w], move: true) }
merged = {}
NW.times do
  out.receive.each do |k, v|
    raise "dup #{k}" if merged.key?(k)
    merged[k] = v
  end
end
ws.each(&:value)
raise "merged=#{merged}" unless merged == exp
puts "OK b34_groupby_move_parts"
