# hash join の build 後・probe 前に全 worker が GC.compact する (表の移動耐性)
# axes: 2 workers, copy, GC.compact between phases
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

NW = 2
NA = 20
NB = 20
exp = 0
NA.times do |i|
  k = i % 7
  NB.times { |j| exp += i * j if k == (j * 3) % 7 }
end

out = Ractor::Port.new
ws = NW.times.map do
  Ractor.new(out) do |o|
    build = Hash.new { |h, k| h[k] = [] }
    sum = 0
    loop do
      msg = Ractor.receive
      break if msg == :stop
      case msg[0]
      when :build then build[msg[1]] << msg[2]
      when :compact then GC.compact
      when :probe
        build[msg[1]].each { |bv| sum += bv * msg[2] } if build.key?(msg[1])
      end
    end
    o.send(sum)
  end
end
NA.times { |i| ws[(i % 7) % NW].send([:build, i % 7, i]) }
ws.each { |w| w.send([:compact]) }
GC.compact
NB.times { |j| ws[((j * 3) % 7) % NW].send([:probe, (j * 3) % 7, j]) }
ws.each { |w| w.send(:stop) }
got = 0
NW.times { got += out.receive }
ws.each(&:value)
raise "got=#{got} exp=#{exp}" unless got == exp
puts "OK b38_join_compact_mid"
