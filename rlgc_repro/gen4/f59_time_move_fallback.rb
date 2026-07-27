# f59 scheduler: moving Time raises Ractor::Error (rescued, asserted); COPY fallback delivers it
# axes: move-reject rescue path, copy fallback, GC.start
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
    po.send([mm[:at].to_i, mm[:at].nsec, mm[:at].utc?, mm[:job]])
  end
end

now = Time.now
job = { job: :backup, at: now }

# 1. move must be rejected for Time
begin
  w.send(job, move: true)
  raise "moving Time unexpectedly succeeded"
rescue Ractor::Error => err
  assert err.message.include?("move"), "error mentions move: #{err.message}"
end
# rejection must leave the source untouched (no husk)
assert job[:at].equal?(now) && now.to_i > 0, "source intact after rejected move"

# 2. copy fallback works
w.send(job)
GC.start
ti, tn, tu, jname = port.receive
assert ti == now.to_i, "epoch seconds"
assert tn == now.nsec, "nanoseconds"
assert tu == now.utc?, "utc flag"
assert jname == :backup, "job tag"
w.send(:eof)
puts "OK f59_time_move_fallback"
