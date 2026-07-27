# f77 kitchen sink: one payload with structs, data, exceptions, ranges, rationals, encodings,
# genivars, nested depth-5 -- full deep equality round-trip
# axes: copy, everything-bag, GC.start + GC.compact
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

Pt = Struct.new(:x, :y)
Ev = Data.define(:name, :at)

def bag
  tagged = +"tagged-str"
  tagged.instance_variable_set(:@tag, :hot)
  {
    meta: { v: 3, layers: { a: { b: { c: [1, [2, [3]]] } } } },
    pts: [Pt.new(1, 2), Pt.new(-3, 4)],
    evs: [Ev.new(name: :boot, at: 0), Ev.new(name: :halt, at: 99)],
    errs: [ArgumentError.new("bag-err"), RuntimeError.new("bag-run")],
    nums: [Rational(3, 8), Complex(1, 1), 2**66, -4.5],
    rngs: [(1..5), ('a'..'c'), (10..)],
    strs: ["uni-こんにちは", "\x01\x02".b, tagged],
    flags: [true, false, nil, :sym],
  }
end

port = Ractor::Port.new
w = Ractor.new(port) do |po|
  mm = Ractor.receive
  GC.compact
  po.send([mm, mm[:strs][2].instance_variable_get(:@tag),
           mm[:errs].map(&:message), mm[:strs][1].encoding.name])
end

original = bag
w.send(original)
GC.start
back, tag, errmsgs, binenc = port.receive
assert back.keys == original.keys, "top keys"
assert back[:meta] == original[:meta], "deep nest"
assert back[:pts] == original[:pts], "structs"
assert back[:evs] == original[:evs], "data objects"
assert errmsgs == ["bag-err", "bag-run"], "exceptions"
assert back[:nums] == original[:nums], "numerics"
assert back[:rngs] == original[:rngs], "ranges"
assert back[:strs][0] == original[:strs][0] && back[:strs][1] == original[:strs][1], "strings"
assert tag == :hot, "generic ivar traveled"
assert binenc == "ASCII-8BIT", "binary encoding kept"
assert back[:flags] == [true, false, nil, :sym], "flags"
puts "OK f77_mega_payload_roundtrip"
