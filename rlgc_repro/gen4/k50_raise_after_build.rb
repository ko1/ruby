# 重い割り当ての途中で raise、GC.compact 中に RemoteError 回収
# axes: raisework,alloc,after
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
N = 12
workers = N.times.map do |k|
  Ractor.new(k) do |id|
    Thread.current.report_on_exception = false
    acc = []
    16.times do |j|
      acc << (+"chunk-#{id}-#{j}") * 4
      raise "midway-#{id}" if id.odd? && j == 8
    end
    acc.sum(&:bytesize)
  end
end
fails = 0
oks = 0
workers.each_with_index do |w, k|
  begin
    v = w.value
    oks += 1
    raise "v" unless v > 0
  rescue Ractor::RemoteError
    fails += 1
  end
  GC.compact if k == 6
end
raise "count" unless fails == N / 2 && oks == N / 2
puts "OK k50_raise_after_build"
