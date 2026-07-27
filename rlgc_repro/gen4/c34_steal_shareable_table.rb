# c34: work stealing over a frozen shareable task table (queues carry indices,
# workers read TABLE directly); main stress + final compact.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

W = STRESS ? 3 : 4
T = STRESS ? 9 : 36
TABLE = Ractor.make_shareable(Array.new(T) { |i| { id: i, w: i * 5 + 3, name: "t#{i}" } })

coord = Ractor.new(W, T) do |w, t|
  queues = Array.new(w) { |i| ((i * t / w)...((i + 1) * t / w)).to_a }
  dones = 0
  handed = 0
  while dones < w
    tag, wid, reply = Ractor.receive
    raise "tag" unless tag == :get
    idx = queues[wid].shift
    if idx.nil?
      qi = queues.index { |q| !q.empty? }
      idx = queues[qi].pop if qi
    end
    if idx
      handed += 1
      reply << [:idx, idx]
    else
      dones += 1
      reply << [:done]
    end
  end
  handed
end

res = Ractor::Port.new
ws = W.times.map do |i|
  Ractor.new(coord, res, i) do |c, rp, wid|
    my = Ractor::Port.new
    acc = 0
    seen = []
    loop do
      c.send([:get, wid, my])
      msg = my.receive
      break if msg[0] == :done
      e = TABLE[msg[1]]
      raise "table" unless e[:id] == msg[1] && e[:name] == "t#{msg[1]}"
      acc += e[:w]
      seen << msg[1]
    end
    rp << [:res, wid, acc, seen]
  end
end

sum = 0
all = []
W.times do
  tag, _, acc, seen = res.receive
  raise "res" unless tag == :res
  sum += acc
  all.concat(seen)
end
raise "all" unless all.sort == (0...T).to_a
raise "sum" unless sum == (0...T).sum { |i| i * 5 + 3 }
GC.stress = false
raise unless coord.value == T
ws.each(&:value)
GC.compact
puts "OK c34_steal_shareable_table"
