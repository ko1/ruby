# c48: scatter-gather: main scatters value chunks to W workers (copy), gathers
# tagged partial sums on one port, verifies the exact total.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

W = STRESS ? 3 : 4
PER = STRESS ? 10 : 60
DATA = (0...(W * PER)).map { |i| (i * 13) % 97 }

gather = Ractor::Port.new
ws = W.times.map do |i|
  Ractor.new(gather, i) do |g, wid|
    tag, chunk = Ractor.receive
    raise "chunk" unless tag == :chunk
    g << [:partial, wid, chunk.sum, chunk.size]
    :scattered
  end
end
W.times { |i| ws[i].send([:chunk, DATA[i * PER, PER]]) }

total = 0
cnt = 0
seen = {}
W.times do
  tag, wid, s, n = gather.receive
  raise "partial" unless tag == :partial
  raise "dup" if seen[wid]
  seen[wid] = true
  total += s
  cnt += n
end
raise "cnt" unless cnt == W * PER
raise "total" unless total == DATA.sum
GC.stress = false
ws.each { |r| raise unless r.value == :scattered }
puts "OK c48_scatter_sum"
