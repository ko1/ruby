# gen4 map-reduce: chunk arrays are MOVED to workers; each worker mutates its
# chunk in place, computes a partial, and MOVES a result buffer back via port.
# axes: transfer=move (both directions), GC=none, exceptions=none, payload=int arrays
N_WORKERS = 6
CHUNK = 500

results = Ractor::Port.new
workers = N_WORKERS.times.map do |wid|
  Ractor.new(results, wid) do |res, id|
    chunk = Ractor.receive
    chunk.map! { |x| x * 3 }
    buf = [id, chunk.sum, chunk.size]
    res.send(buf, move: true)
    :done
  end
end

expected = 0
N_WORKERS.times do |w|
  chunk = Array.new(CHUNK) { |i| w * CHUNK + i }
  expected += chunk.sum * 3
  workers[w].send(chunk, move: true)
end

total = items = 0
N_WORKERS.times do
  _id, part, size = results.receive
  total += part
  items += size
end
workers.each(&:join)
raise "FAIL items" unless items == N_WORKERS * CHUNK
raise "FAIL total #{total} != #{expected}" unless total == expected
puts "OK mr_sum_move"
