# 多数 Ractor が一斉に raise、収集中に GC.start(20体)
# axes: storm,many,start
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
N = 20
workers = N.times.map do |k|
  Ractor.new(k) do |id|
    Thread.current.report_on_exception = false
    junk = Array.new(8) { +"s-#{id}-#{_1}" }
    raise "storm-#{id}" if true
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
  GC.start if k == N / 2
end
raise 'count' unless fails == N && oks == 0
puts "OK k56_storm_all_raise_20"
