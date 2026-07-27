# severity ヒストグラムを worker 側 stress 付きで集計し main で merge
# axes: 2 workers, worker-side GC.stress, copy, tiny load
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 16
sevs = Array.new(N) { |i| (i * i) % 5 }
exp = Hash.new(0)
sevs.each { |s| exp[s] += 1 }

out = Ractor::Port.new
ws = 2.times.map do
  Ractor.new(out) do |o|
    GC.stress = true if ENV['S_STRESS']
    h = Hash.new(0)
    loop do
      s = Ractor.receive
      break if s == :stop
      h["sev#{s}"] += 1
    end
    GC.stress = false
    o.send(h)
  end
end
sevs.each_with_index { |s, i| ws[i % 2].send(s) }
ws.each { |w| w.send(:stop) }
merged = Hash.new(0)
2.times { out.receive.each { |k, v| merged[k] += v } }
ws.each(&:value)
exp2 = exp.transform_keys { |s| "sev#{s}" }
raise "merged=#{merged}" unless merged == exp2
puts "OK b17_log_severity_hist"
