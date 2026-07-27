# c31: work stealing with all tasks initially on queue 0 (maximally uneven);
# participant-bounded GC.stress; coordinator does bounded GC.start.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false

W = STRESS ? 3 : 4
T = STRESS ? 10 : 36

coord = Ractor.new(W, T) do |w, t|
  queues = Array.new(w) { [] }
  queues[0] = (0...t).to_a
  handed = 0
  dones = 0
  msgs = 0
  while dones < w
    tag, wid, reply = Ractor.receive
    msgs += 1
    GC.start if msgs == t / 2
    raise "tag" unless tag == :get
    task = queues[wid].shift
    if task.nil?
      qi = queues.index { |q| !q.empty? }
      task = queues[qi].pop if qi
    end
    if task
      handed += 1
      reply << [:task, task]
    else
      dones += 1
      reply << [:done]
    end
  end
  raise "handed" unless handed == t
  handed
end

res = Ractor::Port.new
ws = W.times.map do |i|
  Ractor.new(coord, res, i) do |c, rp, wid|
    GC.stress = true if ENV['S_STRESS']
    my = Ractor::Port.new
    acc = 0
    cnt = 0
    loop do
      c.send([:get, wid, my])
      msg = my.receive
      break if msg[0] == :done
      acc += msg[1] * 3 + 1
      cnt += 1
    end
    GC.stress = false
    rp << [:res, wid, acc, cnt]
  end
end

sum = 0
cnt = 0
W.times do
  tag, _, acc, c = res.receive
  raise "res" unless tag == :res
  sum += acc; cnt += c
end
raise "cnt" unless cnt == T
raise "sum" unless sum == (0...T).sum { |x| x * 3 + 1 }
raise unless coord.value == T
ws.each(&:value)
GC.compact unless STRESS
puts "OK c31_steal_uneven"
