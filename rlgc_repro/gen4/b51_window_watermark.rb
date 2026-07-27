# 順序乱れ (ペア swap) のある stream を watermark で flush する window 集計
# axes: 1 worker, copy, out-of-order events + watermark
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 40
W = 5
# 偶奇 swap した決定的な順序乱れ: 1,0,3,2,...
order = (0...N).map { |i| i.even? ? i + 1 : i - 1 }
exp = (0...N).each_slice(W).map { |sl| sl.sum { |t| t * 3 } }

out = Ractor::Port.new
w = Ractor.new(out, W, N) do |o, width, n|
  pend = {}
  maxt = -1
  flushed = 0
  wins = []
  flush = lambda do
    while (flushed + 1) * width <= maxt - 1 # watermark = maxt - 2 の窓まで確定
      lo = flushed * width
      wins << (lo...(lo + width)).sum { |t| pend.delete(t) }
      flushed += 1
    end
  end
  n.times do
    t, v = Ractor.receive
    pend[t] = v
    maxt = t if t > maxt
    flush.call
  end
  # eof: 残り全部 flush
  while flushed * width < n
    lo = flushed * width
    wins << (lo...(lo + width)).sum { |t| pend.delete(t) }
    flushed += 1
  end
  o.send([wins, pend.size])
end
order.each { |t| w.send([t, t * 3]) }
wins, leftover = out.receive
w.value
raise "leftover=#{leftover}" unless leftover == 0
raise "wins=#{wins} exp=#{exp}" unless wins == exp
puts "OK b51_window_watermark"
