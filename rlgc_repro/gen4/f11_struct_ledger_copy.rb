# f11 ledger service: Struct records copied to bookkeeper worker, totals verified
# axes: copy, Struct, request/response, GC.start mid
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

Entry = Struct.new(:acct, :amount, :memo)

port = Ractor::Port.new
w = Ractor.new(port) do |po|
  loop do
    mm = Ractor.receive
    break if mm == :eof
    tot = mm.sum(&:amount)
    accts = mm.map(&:acct).uniq.sort
    po.send([tot, accts, mm])
  end
end

n = STRESS ? 6 : 30
entries = n.times.map { |i| Entry.new(:"a#{i % 3}", (i + 1) * 10, "memo-#{i}") }
w.send(entries)
GC.start
tot, accts, back = port.receive
assert tot == (1..n).sum * 10, "total #{tot}"
assert accts == [:a0, :a1, :a2], "accounts #{accts.inspect}"
assert back == entries, "struct deep equality"
assert back.first.is_a?(Entry), "class preserved"
w.send(:eof)
puts "OK f11_struct_ledger_copy"
