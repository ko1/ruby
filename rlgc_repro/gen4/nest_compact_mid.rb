# gen4 nested: middle layer runs GC.compact while holding refs to (a) its
# children, (b) partially-gathered child results, and (c) shared frozen input;
# leaves also compact once. Exercises compaction at every tree level.
# axes: transfer=copy, GC=GC.compact at all levels, exceptions=none, depth=2
SHARED = Ractor.make_shareable(Array.new(300) { |i| "shared-#{i}" })

N_SUP = 3
N_LEAF = 3

sups = N_SUP.times.map do |sid|
  Ractor.new(sid, N_LEAF) do |sup_id, nleaf|
    leaves = nleaf.times.map do |lid|
      Ractor.new(sup_id, lid) do |s, l|
        sum = 0
        (0...SHARED.size).step(3) do |i|
          sum += SHARED[i].size + s + l
        end
        GC.compact
        sum
      end
    end
    partial = []
    leaves.each_with_index do |leaf, k|
      partial << leaf.value
      GC.compact if k == 1   # children + partial results live across this
    end
    GC.compact
    partial.sum
  end
end

got = sups.sum(&:value)
GC.compact
exp = 0
N_SUP.times do |s|
  N_LEAF.times do |l|
    (0...SHARED.size).step(3) { |i| exp += SHARED[i].size + s + l }
  end
end
raise "FAIL #{got} != #{exp}" unless got == exp
puts "OK nest_compact_mid"
