# 両側テーブルを key%NW で同じ worker に送る partitioned hash join
# axes: 3 workers, copy, build/probe 2 相を :phase メッセージで区切る
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

NW = 3
NA = 24
NB = 18
exp = 0
NA.times do |i|
  ka = i % 9
  NB.times do |j|
    exp += i + j * 2 if ka == (j * 2) % 9
  end
end

out = Ractor::Port.new
ws = NW.times.map do
  Ractor.new(out) do |o|
    build = Hash.new { |h, k| h[k] = [] }
    sum = 0
    phase = :build
    loop do
      msg = Ractor.receive
      break if msg == :stop
      if msg == :probe_phase
        phase = :probe
      elsif phase == :build
        k, v = msg
        build[k] << v
      else
        k, v = msg
        build[k].each { |bv| sum += bv + v } if build.key?(k)
      end
    end
    o.send(sum)
  end
end
NA.times { |i| ws[(i % 9) % NW].send([i % 9, i]) }
ws.each { |w| w.send(:probe_phase) }
NB.times { |j| ws[((j * 2) % 9) % NW].send([(j * 2) % 9, j * 2]) }
ws.each { |w| w.send(:stop) }
got = 0
NW.times { got += out.receive }
ws.each(&:value)
raise "got=#{got} exp=#{exp}" unless got == exp
puts "OK b33_join_partitioned"
