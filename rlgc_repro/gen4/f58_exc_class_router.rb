# f58 error router: custom exception hierarchy; worker classifies instances by ancestry
# axes: copy, class hierarchy of exception VALUES, GC.start
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

class AppError < StandardError; end
class NetError < AppError; end
class TimeoutErr < NetError; end
class DataError < AppError; end

port = Ractor::Port.new
w = Ractor.new(port) do |po|
  loop do
    mm = Ractor.receive
    break if mm == :eof
    bucket = case mm
             when TimeoutErr then :timeout
             when NetError then :net
             when DataError then :data
             when AppError then :app
             else :unknown
             end
    po.send([bucket, mm.message])
  end
end

cases = [
  [TimeoutErr.new("t1"), :timeout],
  [NetError.new("n1"), :net],
  [DataError.new("d1"), :data],
  [AppError.new("a1"), :app],
  [RuntimeError.new("r1"), :unknown],
]
cases.each { |ee, _| w.send(ee) }
GC.start
cases.each do |ee, want|
  bucket, msgv = port.receive
  assert bucket == want, "#{ee.class}: routed to #{bucket}"
  assert msgv == ee.message, "message preserved"
end
w.send(:eof)
puts "OK f58_exc_class_router"
