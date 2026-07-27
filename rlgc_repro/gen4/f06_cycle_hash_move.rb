# f06 registry app: Hash containing itself (self and nested cycles) moved
# axes: move with hash cycles, GC.compact after receive
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

port = Ractor::Port.new
w = Ractor.new(port) do |po|
  mm = Ractor.receive
  GC.compact
  ok_self = mm[:self].equal?(mm)
  ok_deep = mm[:child][:parent].equal?(mm)
  ok_data = mm[:data] == [10, 20, 30] && mm[:child][:name] == "leaf"
  po.send([ok_self, ok_deep, ok_data])
end

reg = { data: [10, 20, 30] }
reg[:self] = reg
reg[:child] = { name: "leaf", parent: reg }
w.send(reg, move: true)
begin
  reg[:data]
  raise "hash source not husked"
rescue Ractor::MovedError
end
a, b, c = port.receive
assert a, "self-cycle broken"
assert b, "nested parent-cycle broken"
assert c, "payload data lost"
puts "OK f06_cycle_hash_move"
