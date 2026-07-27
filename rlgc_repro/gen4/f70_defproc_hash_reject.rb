# f70 config gate: Hash with default_proc is uncopyable (Ractor::Error rescued); plain default ships
# axes: copy-reject rescue path, Hash defaults, GC.start
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
    po.send([mm[:known], mm[:unknown_key], mm.default])
  end
end

# 1. default_proc hash must be rejected at send
bad = Hash.new { |hh, kk| hh[kk] = "auto-#{kk}" }
bad[:known] = "explicit"
begin
  w.send(bad)
  raise "default_proc hash unexpectedly copyable"
rescue Ractor::Error => err
  assert err.message.include?("copy"), "reject reason: #{err.message}"
end
# source must remain fully functional after the rejected send
assert bad[:autogen] == "auto-autogen", "default_proc still works on source"

# 2. plain default value ships fine
good = Hash.new("fallback")
good[:known] = "explicit"
w.send(good)
GC.start
known, unknown, dflt = port.receive
assert known == "explicit", "explicit entry"
assert unknown == "fallback", "default value used remotely"
assert dflt == "fallback", "default carried"
w.send(:eof)
puts "OK f70_defproc_hash_reject"
