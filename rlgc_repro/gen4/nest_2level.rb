# gen4 nested: main -> 4 supervisors -> each spawns 4 leaf workers; leaf sums
# flow up through supervisor #value to main. Copy everywhere.
# axes: transfer=copy, GC=none, exceptions=none, depth=2
N_SUP = 4
N_LEAF = 4
UNITS = 100

sups = N_SUP.times.map do |sid|
  Ractor.new(sid, N_LEAF, UNITS) do |sup_id, nleaf, units|
    leaves = nleaf.times.map do |lid|
      Ractor.new(sup_id, lid, units) do |s, l, u|
        base = (s * 10 + l) * 1000
        (0...u).sum { |i| base + i }
      end
    end
    leaves.sum(&:value)
  end
end

got = sups.sum(&:value)
exp = 0
N_SUP.times do |s|
  N_LEAF.times do |l|
    base = (s * 10 + l) * 1000
    exp += (0...UNITS).sum { |i| base + i }
  end
end
raise "FAIL #{got} != #{exp}" unless got == exp
puts "OK nest_2level"
