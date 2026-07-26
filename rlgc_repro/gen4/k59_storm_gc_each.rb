# 多数 Ractor が一斉に raise、収集中に GC.start(full_mark: true)(16体)
# axes: storm,many,gceach
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
N = 16
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
  GC.start(full_mark: true) if k == N / 2
end
raise 'count' unless fails == N && oks == 0
puts "OK k59_storm_gc_each"
