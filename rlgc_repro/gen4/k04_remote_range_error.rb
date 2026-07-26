# worker が RangeError を raise し .value が Ractor::RemoteError に包む
# axes: prop,rangeerr,value
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
N = 12
EXC = RangeError
workers = N.times.map do |k|
  Ractor.new(k) do |id|
    Thread.current.report_on_exception = false
    buf = Array.new(8) { +"payload-#{id}-#{_1}" }
    raise EXC, "boom-#{id}" if id.odd?
    buf.sum(&:bytesize)
  end
end
fails = 0
oks = 0
workers.each_with_index do |w, k|
  begin
    v = w.value
    oks += 1
    raise "bad" unless v > 0
  rescue Ractor::RemoteError => rex
    fails += 1
    raise "wrong cause" unless rex.cause.is_a?(EXC)
  end
  GC.compact if k == 6
end
raise "count" unless fails == N / 2 && oks == N / 2
puts "OK k04_remote_range_error"
