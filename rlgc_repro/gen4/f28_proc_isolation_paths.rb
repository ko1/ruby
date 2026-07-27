# f28 plugin loader: proc shareability triage — captured proc rejected, raw proc uncopyable, pure lambda ok
# axes: Ractor::IsolationError + Ractor::Error rescue paths, then working shareable proc
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

module Plugins
  def self.captures_unshareable
    buf = +"mutable-buffer"
    ->(x) { buf << x } # refers to an unshareable object
  end
  def self.assigns_outer
    z = 0
    ->(x) { z = x } # writes an outer local
  end
  def self.pure
    ->(x) { x * 3 }
  end
end

# 1. make_shareable on non-isolable procs must raise IsolationError
begin
  Ractor.make_shareable(Plugins.captures_unshareable)
  raise "unshareable-capturing proc unexpectedly shareable"
rescue Ractor::IsolationError
end
begin
  Ractor.make_shareable(Plugins.assigns_outer)
  raise "outer-assigning proc unexpectedly shareable"
rescue Ractor::IsolationError
end

# 2. sending a raw (unshareable) proc must raise Ractor::Error
sink = Ractor.new do
  loop do
    mm = Ractor.receive
    break if mm == :eof
  end
end
begin
  sink.send(Plugins.pure)
  raise "raw proc unexpectedly copyable"
rescue Ractor::Error
end
sink.send(:eof)

# 3. shareable pure lambda works cross-ractor
fn = Ractor.make_shareable(Plugins.pure)
port = Ractor::Port.new
Ractor.new(port, fn) { |po, ff| po.send(ff.call(14)) }
GC.start
assert port.receive == 42, "shareable lambda result"
puts "OK f28_proc_isolation_paths"
