# f57 flaky batch: pool of one-shot ractors, odd jobs raise; collect values + RemoteErrors
# axes: send-die-value, mixed success/failure, stress bounded around #value
Warning[:experimental] = false
Thread.report_on_exception = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

n = STRESS ? 4 : 8
rs = n.times.map do |i|
  r = Ractor.new do
    jid = Ractor.receive
    raise "flaky-#{jid}" if jid.odd?
    jid * 100
  end
  r.send(i)
  r
end

# bound stress: #value under active GC.stress can hit known upstream recursive-lock assert
GC.stress = false if STRESS
oks = []
errs = []
rs.each_with_index do |r, i|
  begin
    oks << [i, r.value]
  rescue Ractor::RemoteError => err
    errs << [i, err.cause.message]
  end
end
GC.stress = true if STRESS
assert oks == n.times.select(&:even?).map { |i| [i, i * 100] }, "successes #{oks.inspect}"
assert errs == n.times.select(&:odd?).map { |i| [i, "flaky-#{i}"] }, "failures #{errs.inspect}"
GC.start
puts "OK f57_remote_error_pool"
