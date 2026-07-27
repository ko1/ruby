# key ごとの tumbling window (幅 4) を key-partition した 3 worker が処理
# axes: 3 workers (disjoint keys), copy, per-key window list
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

NK = 6
N = 72
events = Array.new(N) { |i| [i % NK, (i * 7) % 40] }
exp = Hash.new { |h, k| h[k] = [] }
tmp = Hash.new { |h, k| h[k] = [] }
events.each do |k, v|
  tmp[k] << v
  if tmp[k].size == 4
    exp[k] << tmp[k].sum
    tmp[k] = []
  end
end
exp = exp.to_h { |k, v| [k, v] }

out = Ractor::Port.new
ws = 3.times.map do
  Ractor.new(out) do |o|
    bufs = Hash.new { |h, k| h[k] = [] }
    wins = Hash.new { |h, k| h[k] = [] }
    loop do
      msg = Ractor.receive
      break if msg == :stop
      k, v = msg
      bufs[k] << v
      if bufs[k].size == 4
        wins[k] << bufs[k].sum
        bufs[k] = []
      end
    end
    o.send(wins.to_h { |k, v| [k, v] })
  end
end
events.each { |k, v| ws[k % 3].send([k, v]) }
ws.each { |w| w.send(:stop) }
merged = {}
3.times do
  out.receive.each do |k, v|
    raise "dup #{k}" if merged.key?(k)
    merged[k] = v
  end
end
ws.each(&:value)
raise "merged=#{merged} exp=#{exp}" unless merged == exp
puts "OK b49_window_per_key"
