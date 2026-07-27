# 文字列長ヒストグラム: worker が batch の長さ分布を数え、merge を算術期待値と比較
# axes: 3 workers, copy batches of strings
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 45
strs = Array.new(N) { |i| "k" * (i % 9 + 1) }
exp = Array.new(10, 0)
N.times { |i| exp[i % 9 + 1] += 1 }

out = Ractor::Port.new
ws = 3.times.map do
  Ractor.new(out) do |o|
    h = Array.new(10, 0)
    loop do
      batch = Ractor.receive
      break if batch == :stop
      batch.each { |s| h[s.size] += 1 }
    end
    o.send(h)
  end
end
strs.each_slice(5).with_index { |sl, i| ws[i % 3].send(sl) }
ws.each { |w| w.send(:stop) }
got = Array.new(10, 0)
3.times { out.receive.each_with_index { |c, i| got[i] += c } }
ws.each(&:value)
raise "hist=#{got} exp=#{exp}" unless got == exp
puts "OK b65_hist_strlen"
