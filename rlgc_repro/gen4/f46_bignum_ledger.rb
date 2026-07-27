# f46 crypto-ish ledger: 200-bit integers accumulated across a 2-ractor chain
# axes: copy, bignum arithmetic both sides, chain, GC.start
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

out = Ractor::Port.new
adder = Ractor.new(out) do |po|
  acc = 0
  loop do
    mm = Ractor.receive
    if mm == :eof
      po.send(acc)
      break
    end
    acc += mm
  end
end
feeder = Ractor.new(adder) do |nxt|
  loop do
    mm = Ractor.receive
    if mm == :eof
      nxt.send(:eof)
      break
    end
    nxt.send(mm * 3) # bignum multiply inside ractor
  end
end

base = 2**200 + 12345
n = STRESS ? 3 : 10
n.times { |i| feeder.send(base + i) }
feeder.send(:eof)
GC.start
total = out.receive
want = n.times.sum { |i| (base + i) * 3 }
assert total == want, "bignum total mismatch"
assert total > 2**201, "still a bignum"
puts "OK f46_bignum_ledger"
