# c30: work stealing with moved task payloads (mutable arrays move coordinator->
# worker; result arrays move worker->main).
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

W = STRESS ? 3 : 4
T = STRESS ? 9 : 32

coord = Ractor.new(W, T) do |w, t|
  queues = Array.new(w) { |i| [] }
  t.times { |x| queues[x % w] << [x, "task-#{x}"] }
  handed = 0
  dones = 0
  while dones < w
    tag, wid, reply = Ractor.receive
    raise "tag" unless tag == :get
    task = queues[wid].shift
    if task.nil?
      qi = queues.index { |q| !q.empty? }
      task = queues[qi].pop if qi
    end
    if task
      handed += 1
      reply.send([:task, task], move: true)
    else
      reply << [:done]
      dones += 1
    end
  end
  raise "handed" unless handed == t
  handed
end

res = Ractor::Port.new
ws = W.times.map do |i|
  Ractor.new(coord, res, i) do |c, rp, wid|
    my = Ractor::Port.new
    got = []
    loop do
      c.send([:get, wid, my])
      msg = my.receive
      break if msg[0] == :done
      x, s = msg[1]
      raise "body" unless s == "task-#{x}"
      got << x
    end
    rp.send([:res, wid, got], move: true)
  end
end

all = []
W.times do
  tag, _, got = res.receive
  raise "res" unless tag == :res
  all.concat(got)
end
raise "all" unless all.sort == (0...T).to_a
GC.stress = false
raise unless coord.value == T
ws.each(&:value)
puts "OK c30_steal_move"
