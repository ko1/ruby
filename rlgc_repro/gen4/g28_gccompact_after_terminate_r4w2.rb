# spawn 2 waves of 4 ractors returning a checksum; force GC.compact right after each wave joins (post-termination window)
# axes: 4 ractors/wave, 2 waves, GC.compact, gc immediately after termination
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
NR = 4
WAVES = 2
total = 0
WAVES.times do |w|
  rs = NR.times.map do |i|
    Ractor.new(w, i) do |ww, id|
      Array.new(12) { |j| +"x#{ww}-#{id}-#{j}" }.sum(&:bytesize)
    end
  end
  total += rs.sum(&:value)
  GC.compact
end
exp = 0
WAVES.times do |w|
  NR.times do |i|
    exp += Array.new(12) { |j| "x#{w}-#{i}-#{j}".bytesize }.sum
  end
end
raise "total=#{total} exp=#{exp}" unless total == exp
puts "OK g28_gccompact_after_terminate_r4w2"
