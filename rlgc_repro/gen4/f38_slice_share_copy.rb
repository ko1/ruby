# f38 excerpt service: substrings sliced from one big backing string, copied out
# axes: copy, shared-root strings (COW slices), GC.compact after slicing
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

port = Ractor::Port.new
w = Ractor.new(port) do |po|
  mm = Ractor.receive
  po.send(mm.map { |ss| [ss, ss.bytesize, ss.upcase] })
end

backing = (0...64).map { |i| "seg#{format('%02d', i)}." }.join # 64 * 6 bytes
nslices = STRESS ? 4 : 8
slices = nslices.times.map { |i| backing[i * 48, 24] } # COW-ish shared slices
GC.compact
w.send(slices)
back = port.receive
back.each_with_index do |(txt, bs, up), i|
  assert txt == backing[i * 48, 24], "slice #{i} content"
  assert bs == 24, "slice #{i} bytesize"
  assert up == backing[i * 48, 24].upcase, "slice #{i} transform"
end
# backing string unharmed
assert backing.bytesize == 64 * 6, "backing intact"
puts "OK f38_slice_share_copy"
