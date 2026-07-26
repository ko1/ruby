# 深い call stack から raise→cause.backtrace を検証(depth 3)
# axes: backtrace,cause,message
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
N = 12
DEPTH = 3
workers = N.times.map do |k|
  Ractor.new(k) do |id|
    Thread.current.report_on_exception = false
    deep = lambda do |n|
      if n <= 0
        raise "bottom-#{id}"
      else
        deep.call(n - 1)
      end
    end
    deep.call(DEPTH)
  end
end
fails = 0
workers.each_with_index do |w, k|
  begin
    w.value
  rescue Ractor::RemoteError => rex
    fails += 1
    bt = rex.cause.backtrace
    raise "bt" unless bt.is_a?(Array) && bt.length >= DEPTH
    raise "msg" unless rex.cause.message == "bottom-#{k}"
  end
  GC.compact if k == 5
end
raise "count" unless fails == N
puts "OK k48_backtrace_message"
