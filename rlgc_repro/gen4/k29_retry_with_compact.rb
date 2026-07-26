# worker が 4 回目で成功するまで retry(例外再試行)
# axes: retry,rescue,compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
N = 12
TRIES = 4
workers = N.times.map do |k|
  Ractor.new(k) do |id|
    Thread.current.report_on_exception = false
    n = 0
    begin
      n += 1
      scratch = Array.new(6) { +"r-#{id}-#{n}-#{_1}" }
      raise "retry-#{id}" if n < TRIES
      [scratch.size, n]
    rescue
      retry
    end
  end
end
res = workers.map(&:value)
GC.compact
raise "attempts" unless res.all? { |(_sz, n)| n == TRIES }
puts "OK k29_retry_with_compact"
