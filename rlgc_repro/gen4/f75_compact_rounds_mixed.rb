# f75 defrag victim: mixed graph re-sent every round with GC.compact on both sides between rounds
# axes: copy, GC.compact heavy, long-lived worker state
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

Blob = Struct.new(:seq, :data)

port = Ractor::Port.new
w = Ractor.new(port) do |po|
  kept = []
  loop do
    mm = Ractor.receive
    break if mm == :eof
    kept << mm
    GC.compact
    po.send([kept.size, kept.sum { |bb| bb.seq }, mm.data[:strs].join(","), kept.first.data[:strs][0]])
  end
end

rounds = STRESS ? 3 : 6
rounds.times do |i|
  blob = Blob.new(i, { strs: ["s#{i}a", "s#{i}b"], nums: [i, i * 2] })
  w.send(blob)
  GC.compact
  cnt, seqsum, joined, first0 = port.receive
  assert cnt == i + 1, "worker kept #{cnt}"
  assert seqsum == (0..i).sum, "seq sum"
  assert joined == "s#{i}a,s#{i}b", "current round data"
  assert first0 == "s0a", "oldest retained blob survives worker-side compactions"
end
w.send(:eof)
puts "OK f75_compact_rounds_mixed"
