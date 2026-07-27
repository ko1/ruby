# f47 sharder: integer range partitioned into subranges handed to 3 workers, sums fan in
# axes: copy, Range payloads, pool fan-out/fan-in, GC.start
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

port = Ractor::Port.new
pool = 3.times.map do |wi|
  Ractor.new(port, wi) do |po, myid|
    loop do
      mm = Ractor.receive
      break if mm == :eof
      po.send([myid, mm, mm.sum])
    end
  end
end

whole = STRESS ? (1..30) : (1..3000)
step = whole.size / 3
parts = 3.times.map { |i| (whole.begin + i * step)..(i == 2 ? whole.end : whole.begin + (i + 1) * step - 1) }
parts.each_with_index { |pp, i| pool[i].send(pp) }
GC.start
total = 0
3.times do
  wid, rng, sum = port.receive
  assert rng == parts[wid], "worker #{wid} got its own shard back"
  assert sum == rng.sum, "shard sum"
  total += sum
end
assert total == whole.sum, "partition covers whole range exactly"
pool.each { |w| w.send(:eof) }
puts "OK f47_range_partition_pool"
