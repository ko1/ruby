# 多数 Ractor が一斉に raise、収集中に GC.compact(16体)
# axes: storm,half,compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
N = 16
workers = N.times.map do |k|
  Ractor.new(k) do |id|
    Thread.current.report_on_exception = false
    junk = Array.new(8) { +"s-#{id}-#{_1}" }
    raise "storm-#{id}" if id.even?
    junk.size
  end
end
fails = 0
oks = 0
workers.each_with_index do |w, k|
  begin
    w.value
    oks += 1
  rescue Ractor::RemoteError
    fails += 1
  end
  GC.compact if k == N / 2
end
raise 'count' unless fails == (N + 1) / 2
puts "OK k57_storm_half_raise_16"
