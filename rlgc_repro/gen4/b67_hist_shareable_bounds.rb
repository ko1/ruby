# 不等幅 bucket 境界を frozen shareable 表で共有し、find_index で bucket 決定
# axes: 4 workers, shareable bounds, copy
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

BOUNDS = Ractor.make_shareable([10, 25, 45, 70, 100]) # 5 buckets (v < bound)
N = 60
NW = 4
vals = Array.new(N) { |i| (i * 41 + 3) % 100 }
exp = Array.new(5, 0)
vals.each { |v| exp[BOUNDS.find_index { |b| v < b }] += 1 }

out = Ractor::Port.new
ws = NW.times.map do
  Ractor.new(out, BOUNDS) do |o, bounds|
    h = Array.new(5, 0)
    loop do
      sl = Ractor.receive
      break if sl == :stop
      sl.each { |v| h[bounds.find_index { |b| v < b }] += 1 }
    end
    o.send(h)
  end
end
vals.each_slice(6).with_index { |sl, i| ws[i % NW].send(sl) }
ws.each { |w| w.send(:stop) }
got = Array.new(5, 0)
NW.times { out.receive.each_with_index { |c, i| got[i] += c } }
ws.each(&:value)
raise "hist=#{got} exp=#{exp}" unless got == exp
puts "OK b67_hist_shareable_bounds"
