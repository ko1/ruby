# f35 dedup service: frozen strings in several encodings; copy keeps encoding+frozen; worker dedups
# axes: copy, frozen strings, encodings, GC.compact
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
  uniq = mm.uniq
  po.send([uniq.size, uniq.map { |ss| [ss, ss.encoding.name, ss.frozen?] }])
end

pool = [
  "dup-me".freeze, "dup-me".freeze,      # equal frozen literals
  "バイナリ".freeze, "バイナリ".freeze,
  "\x00\x01".b.freeze, "\x00\x01".b.freeze,
]
w.send(pool)
cnt, uniq = port.receive
assert cnt == 3, "dedup count #{cnt}"
encs = uniq.map { |_, ee, _| ee }.sort
assert encs == ["ASCII-8BIT", "UTF-8", "UTF-8"], "encodings #{encs.inspect}"
assert uniq.all? { |_, _, fz| fz }, "all copies frozen"
assert uniq.map(&:first).include?("dup-me"), "content survived"
puts "OK f35_frozen_enc_dedup"
