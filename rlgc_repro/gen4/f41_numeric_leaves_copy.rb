# f41 metrics ingester: integer (fixnum+bignum), float (incl NaN/Inf) leaves, copy audit
# axes: copy, numeric leaves, GC.start
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

port = Ractor::Port.new
w = Ractor.new(port) do |po|
  mm = Ractor.receive
  po.send([mm[:ints].sum, mm[:big] + 1, mm[:floats][0] * 2,
           mm[:floats][1].nan?, mm[:floats][2].infinite?, mm[:neg].abs])
end

payload = {
  ints: [1, -5, 2**30, 42],
  big: 2**100,
  floats: [1.25, Float::NAN, Float::INFINITY],
  neg: -(2**77),
}
w.send(payload)
GC.start
isum, bplus, fdbl, isnan, isinf, nabs = port.receive
assert isum == 1 - 5 + 2**30 + 42, "int sum"
assert bplus == 2**100 + 1, "bignum arithmetic on copy"
assert fdbl == 2.5, "float"
assert isnan, "NaN travels"
assert isinf == 1, "Infinity travels"
assert nabs == 2**77, "negative bignum"
# bignum copy has same value
assert payload[:big] == 2**100, "source intact"
puts "OK f41_numeric_leaves_copy"
