# c49: scatter-gather with move: chunks moved to workers, transformed result
# arrays moved back; source arrays are relinquished by main.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

W = STRESS ? 3 : 4
PER = STRESS ? 8 : 40

gather = Ractor::Port.new
ws = W.times.map do |i|
  Ractor.new(gather, i) do |g, wid|
    tag, chunk = Ractor.receive
    raise "chunk" unless tag == :chunk
    chunk.map! { |x| x * 2 + 1 }
    g.send([:partial, wid, chunk], move: true)
    :scattered
  end
end
W.times do |i|
  chunk = Array.new(PER) { |k| i * PER + k }
  ws[i].send([:chunk, chunk], move: true)
end

out = Array.new(W)
W.times do
  tag, wid, arr = gather.receive
  raise "partial" unless tag == :partial
  arr << :checked
  raise "mark" unless arr.pop == :checked
  out[wid] = arr
end
W.times do |i|
  raise "w#{i}" unless out[i] == Array.new(PER) { |k| (i * PER + k) * 2 + 1 }
end
GC.stress = false
ws.each { |r| raise unless r.value == :scattered }
GC.start
puts "OK c49_scatter_move"
