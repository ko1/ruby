# 4 leaf hist -> 2 merger -> main の木状 merge (merger は leaf の部分 hist を受けて加算)
# axes: 6 ractors tree, copy, 2 段 merge
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 64
vals = Array.new(N) { |i| (i * 53 + 7) % 80 }
exp = Array.new(8, 0)
vals.each { |v| exp[v / 10] += 1 }

mergers = 2.times.map do
  Ractor.new do
    h = Array.new(8, 0)
    2.times do
      part = Ractor.receive
      part.each_with_index { |c, i| h[i] += c }
    end
    h
  end
end
leaves = 4.times.map do |li|
  Ractor.new(mergers[li / 2], vals[li * 16, 16]) do |dst, part|
    h = Array.new(8, 0)
    part.each { |v| h[v / 10] += 1 }
    dst.send(h)
    :leaf_done
  end
end
leaves.each(&:value)
got = Array.new(8, 0)
mergers.each { |m| m.value.each_with_index { |c, i| got[i] += c } }
raise "hist=#{got} exp=#{exp}" unless got == exp
puts "OK b64_hist_merge_tree"
