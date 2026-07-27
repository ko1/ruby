# chain head->mid->tail; mid dies after 2 jobs; main respawns mid and rewires head (tagged msg)
# axes: deterministic mid-node death+respawn, rewire while chain keeps state at head/tail
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

done = Ractor::Port.new
tail = Ractor.new(done) do |dport|
  loop do
    m = Ractor.receive
    if m == :stop
      dport.send(:stop)
      break
    end
    dport.send(m + 1)
  end
  :fin
end

def spawn_mid(gen, tail)
  Ractor.new(gen, tail) do |g, nx|
    2.times do
      m = Ractor.receive
      nx.send(m + g * 1000)
    end
    :fin
  end
end

head = Ractor.new(tail) do |tl|
  nxt = nil
  cnt = 0
  loop do
    m = Ractor.receive
    case m
    in [:rewire, r] then nxt = r
    in :stop
      tl.send(:stop) # mids are already dead; stop goes straight to tail
      break
    in Integer
      nxt.send(m + 1)
      cnt += 1
    end
  end
  cnt
end

mid1 = spawn_mid(1, tail)
head.send([:rewire, mid1])
2.times { |k| head.send(k * 10) }
got = []
2.times { got << done.receive }
raise "gen1 #{got}" unless got == [0 * 10 + 1 + 1000 + 1, 1 * 10 + 1 + 1000 + 1]
mid2 = spawn_mid(2, tail)
head.send([:rewire, mid2])
2.times { |k| head.send(k * 10) }
got = []
2.times { got << done.receive }
raise "gen2 #{got}" unless got == [0 * 10 + 1 + 2000 + 1, 1 * 10 + 1 + 2000 + 1]
head.send(:stop)
raise unless done.receive == :stop
GC.stress = false
raise unless mid1.value == :fin
raise unless mid2.value == :fin
raise unless head.value == 4
raise unless tail.value == :fin
puts "OK e31_chain_mid_respawn"
