# access-log 風の行から URL ヒット数を数え上位 3 を厳密比較 (tie-break は URL 昇順)
# axes: 4 workers, copy, deterministic top-3
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 60
NW = 4
urls = Array.new(7) { |u| "/p/#{u}" }.freeze
lines = Array.new(N) { |i| "GET #{urls[(i * i + i) % 7]} 200 #{i}" }
seq = Hash.new(0)
N.times { |i| seq[urls[(i * i + i) % 7]] += 1 }
exp_top = seq.sort_by { |u, c| [-c, u] }.first(3)

out = Ractor::Port.new
ws = NW.times.map do
  Ractor.new(out) do |o|
    h = Hash.new(0)
    loop do
      batch = Ractor.receive
      break if batch == :stop
      batch.each { |l| h[l.split(" ")[1]] += 1 }
    end
    o.send(h)
  end
end
lines.each_slice(6).with_index { |sl, i| ws[i % NW].send(sl) }
ws.each { |w| w.send(:stop) }
merged = Hash.new(0)
NW.times { out.receive.each { |k, v| merged[k] += v } }
ws.each(&:value)
GC.compact
top = merged.sort_by { |u, c| [-c, u] }.first(3)
raise "top=#{top.inspect}" unless top == exp_top
raise "total" unless merged.values.sum == N
puts "OK b13_log_url_hits"
