# f24 registry sync: generic ivars on Hashes (metadata sidecar), copy both directions
# axes: copy, generic ivars on Hash, worker mutates and returns, GC.start
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
    mm[:touched] = true
    mm.instance_variable_set(:@version, mm.instance_variable_get(:@version) + 1)
    po.send(mm)
  end
end

reg = { alpha: 1, beta: 2 }
reg.instance_variable_set(:@version, 41)
reg.instance_variable_set(:@owner, :main)
w.send(reg)
GC.start
back = port.receive
assert back == { alpha: 1, beta: 2, touched: true }, "hash content #{back.inspect}"
assert back.instance_variable_get(:@version) == 42, "worker-bumped ivar"
assert back.instance_variable_get(:@owner) == :main, "carried ivar"
# source unaffected by remote mutation
assert reg.instance_variable_get(:@version) == 41 && !reg.key?(:touched), "source isolated"
w.send(:eof)
puts "OK f24_genivar_hash_registry"
