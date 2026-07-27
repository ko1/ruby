# f80 grand finale: pool + chain + send-die-value + compact, mixed payload kinds in one app
# axes: copy+move, pools, chain, #value (stress bounded), GC.start/GC.compact scattered
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

Item = Struct.new(:id, :text)

# stage: pool of 2 normalizers -> single collector; collector ends after both workers drain
collect_port = Ractor::Port.new
collector = Ractor.new(collect_port) do |po|
  seen = []
  done = 0
  loop do
    mm = Ractor.receive
    if mm == :worker_done
      done += 1
      break if done == 2
      next
    end
    seen << mm
    GC.compact if seen.size == 2
  end
  po.send(seen.sort_by(&:id), move: true)
end
pool = 2.times.map do |wi|
  Ractor.new(collector, wi) do |sink, myid|
    loop do
      mm = Ractor.receive
      if mm == :eof
        sink.send(:worker_done)
        break
      end
      mm.text = mm.text.strip.downcase
      mm.instance_variable_set(:@via, myid)
      sink.send(mm, move: true)
    end
  end
end

n = STRESS ? 4 : 8
n.times do |i|
  item = Item.new(i, "  TeXT-#{i}  ")
  pool[i % 2].send(item, move: true)
  begin
    item.id
    raise "item not husked"
  rescue Ractor::MovedError
  end
  GC.start if i == 1
end
pool.each { |w| w.send(:eof) }
sorted = collect_port.receive
assert sorted.map(&:id) == (0...n).to_a, "all items collected in order"
sorted.each_with_index do |it, i|
  assert it.text == "text-#{i}", "normalized text #{it.text.inspect}"
  assert it.instance_variable_get(:@via) == i % 2, "stamped by routed worker"
end

# one-shot summarizer consumes the collected batch (send-die-value)
summarizer = Ractor.new do
  batch = Ractor.receive
  { count: batch.size, chars: batch.sum { |it| it.text.length } }
end
summarizer.send(sorted, move: true)
GC.stress = false if STRESS # bound stress around #value (known upstream assert)
summary = summarizer.value
GC.stress = true if STRESS
assert summary[:count] == n, "summary count"
assert summary[:chars] == (0...n).sum { |i| "text-#{i}".length }, "summary chars"
GC.start
puts "OK f80_grand_finale_mix"
