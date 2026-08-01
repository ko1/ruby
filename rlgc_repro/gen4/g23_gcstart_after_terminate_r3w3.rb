# spawn 3 waves of 3 ractors returning a checksum; force GC.start right after each wave joins (post-termination window)
# axes: 3 ractors/wave, 3 waves, GC.start, gc immediately after termination
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
NR = 3
WAVES = 3
total = 0
WAVES.times do |w|
  rs = NR.times.map do |i|
    Ractor.new(w, i) do |ww, id|
      Array.new(12) { |j| +"x#{ww}-#{id}-#{j}" }.sum(&:bytesize)
    end
  end
  total += rs.sum(&:value)
  GC.start
end
exp = 0
WAVES.times do |w|
  NR.times do |i|
    exp += Array.new(12) { |j| "x#{w}-#{i}-#{j}".bytesize }.sum
  end
end
raise "total=#{total} exp=#{exp}" unless total == exp
puts "OK g23_gcstart_after_terminate_r3w3"
