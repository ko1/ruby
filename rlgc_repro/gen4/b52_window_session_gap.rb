# gap >= 5 で session を区切る sessionization (決定的 timestamp 列)
# axes: 1 worker, copy, session count + per-session sums
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 42
# 7 個ごとに +10 jump する timestamp
ts = []
cur = 0
N.times do |i|
  cur += (i % 7 == 0 && i > 0) ? 10 : 1
  ts << cur
end
exp_sessions = []
start = 0
(1...N).each do |i|
  if ts[i] - ts[i - 1] >= 5
    exp_sessions << (start...i).sum { |j| ts[j] }
    start = i
  end
end
exp_sessions << (start...N).sum { |j| ts[j] }

out = Ractor::Port.new
w = Ractor.new(out) do |o|
  sessions = []
  acc = 0
  last = nil
  while (t = Ractor.receive) != :eof
    if last && t - last >= 5
      sessions << acc
      acc = 0
    end
    acc += t
    last = t
  end
  sessions << acc if last
  o.send(sessions)
end
ts.each { |t| w.send(t) }
w.send(:eof)
got = out.receive
w.value
raise "sessions=#{got.size} exp=#{exp_sessions.size}" unless got.size == exp_sessions.size
raise "sums" unless got == exp_sessions
puts "OK b52_window_session_gap"
