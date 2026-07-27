# gen4 pipeline: 3-stage streaming transform (tokenize -> enrich -> sink).
# Items flow stage-to-stage via default ports; sink reports to main via port.
# axes: transfer=copy, GC=none, exceptions=none, payload=small strings/hashes
N_ITEMS = 500

out = Ractor::Port.new

sink = Ractor.new(out) do |o|
  count = sum = 0
  while (m = Ractor.receive) != :eos
    count += 1
    sum += m[:score]
  end
  o << [count, sum]
end

enrich = Ractor.new(sink) do |nxt|
  while (m = Ractor.receive) != :eos
    nxt << { id: m[:id], words: m[:words], score: m[:words].sum(&:size) }
  end
  nxt << :eos
end

tokenize = Ractor.new(enrich) do |nxt|
  while (m = Ractor.receive) != :eos
    nxt << { id: m[:id], words: m[:line].split("-") }
  end
  nxt << :eos
end

expected = 0
N_ITEMS.times do |i|
  line = "alpha-beta#{i}-gamma-#{i % 10}"
  expected += line.split("-").sum(&:size)
  tokenize << { id: i, line: line }
end
tokenize << :eos

count, sum = out.receive
[tokenize, enrich, sink].each(&:join)
raise "FAIL count #{count}" unless count == N_ITEMS
raise "FAIL sum #{sum} != #{expected}" unless sum == expected
puts "OK pl_3stage_copy"
