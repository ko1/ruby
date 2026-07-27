# f54 resilient worker: rescues internally and reports the exception OBJECT via port (no RemoteError)
# axes: copy of rescued (raised) exceptions incl backtrace, GC.start
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

port = Ractor::Port.new
w = Ractor.new(port) do |po|
  loop do
    mm = Ractor.receive
    break if mm == :eof
    begin
      case mm
      when :div then 1 / 0
      when :key then {}.fetch(:nope)
      else po.send([:ok, mm])
           next
      end
    rescue => ex
      po.send([:err, ex])
    end
  end
end

w.send(:div)
w.send(:key)
w.send(42)
GC.start
tag1, e1 = port.receive
assert tag1 == :err && e1.is_a?(ZeroDivisionError), "div error"
assert e1.message == "divided by 0", "div message"
assert e1.backtrace.is_a?(Array) && !e1.backtrace.empty?, "raised exception carries backtrace strings"
tag2, e2 = port.receive
assert tag2 == :err && e2.is_a?(KeyError), "key error"
assert e2.message.include?("nope"), "key message"
tag3, v3 = port.receive
assert tag3 == :ok && v3 == 42, "healthy path still works"
w.send(:eof)
puts "OK f54_rescued_exc_report"
