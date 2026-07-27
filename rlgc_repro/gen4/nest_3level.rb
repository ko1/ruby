# gen4 nested: 3 levels (main -> 3 mid -> 2 sub each -> 2 leaves each = 12
# leaves); results reduce upward level by level; mid layer also relays a
# progress message stream up to main through a port.
# axes: transfer=copy, GC=GC.start at leaf exit, exceptions=none, depth=3
progress = Ractor::Port.new

mids = 3.times.map do |m|
  Ractor.new(progress, m) do |prog, mid|
    subs = 2.times.map do |s|
      Ractor.new(mid, s) do |mm, ss|
        leaves = 2.times.map do |l|
          Ractor.new(mm, ss, l) do |a, b, c|
            v = (a + 1) * 100 + (b + 1) * 10 + c
            GC.start
            v
          end
        end
        leaves.sum(&:value)
      end
    end
    total = subs.sum(&:value)
    prog << [mid, total]
    total
  end
end

exp_by_mid = 3.times.map do |m|
  2.times.sum { |s| 2.times.sum { |l| (m + 1) * 100 + (s + 1) * 10 + l } }
end

seen = {}
3.times do
  mid, total = progress.receive
  seen[mid] = total
end
got = mids.sum(&:value)
raise "FAIL progress" unless seen == { 0 => exp_by_mid[0], 1 => exp_by_mid[1], 2 => exp_by_mid[2] }
raise "FAIL total" unless got == exp_by_mid.sum
puts "OK nest_3level"
