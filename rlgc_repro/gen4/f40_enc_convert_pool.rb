# f40 recode pool: 2 workers flip strings UTF-8 <-> BINARY (b / force_encoding), results fan in
# axes: copy, encoding conversion in workers, pool, GC.start
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

port = Ractor::Port.new
pool = 2.times.map do |wi|
  Ractor.new(port, wi) do |po, myid|
    loop do
      mm = Ractor.receive
      break if mm == :eof
      id, ss = mm
      if ss.encoding.name == "UTF-8"
        conv = ss.b
      else
        conv = ss.dup.force_encoding("UTF-8")
      end
      po.send([id, myid, conv, conv.encoding.name, conv.valid_encoding?])
    end
  end
end

inputs = [ "みず", "そら", "\xE3\x81\xAF\xE3\x81\x8A".b, "plain-ascii" ]
inputs.each_with_index { |s, i| pool[i % 2].send([i, s]) }
GC.start
res = {}
inputs.size.times do
  id, wid, conv, ename, valid = port.receive
  res[id] = [wid, conv, ename, valid]
end
assert res[0][2] == "ASCII-8BIT" && res[0][0] == 0, "utf8 -> binary on w0"
assert res[1][2] == "ASCII-8BIT", "utf8 -> binary"
assert res[2][2] == "UTF-8" && res[2][3], "binary utf8-bytes -> valid UTF-8"
assert res[2][1] == "はお".b.force_encoding("UTF-8"), "recoded content"
assert res[3][2] == "ASCII-8BIT" && res[3][1] == "plain-ascii".b, "ascii text to binary"
pool.each { |w| w.send(:eof) }
puts "OK f40_enc_convert_pool"
