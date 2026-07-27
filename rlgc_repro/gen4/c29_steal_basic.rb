# c29: work stealing: coordinator holds per-worker deques; empty workers steal
# from lowest-indexed non-empty queue; totals conserved. Copy tasks, main stress.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

W = STRESS ? 3 : 4
T = STRESS ? 9 : 40   # multiple of W

coord = Ractor.new(W, T) do |w, t|
  queues = Array.new(w) { |i| ((i * t / w)...((i + 1) * t / w)).to_a }
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
      reply << [:task, task]
    else
      dones += 1
      reply << [:done]
    end
  end
  raise "handed #{handed}" unless handed == t
  handed
end

res = Ractor::Port.new
ws = W.times.map do |i|
  Ractor.new(coord, res, i) do |c, rp, wid|
    my = Ractor::Port.new
    acc = 0
    cnt = 0
    loop do
      c.send([:get, wid, my])
      msg = my.receive
      break if msg[0] == :done
      raise "task tag" unless msg[0] == :task
      acc += msg[1] * msg[1]
      cnt += 1
    end
    rp << [:res, wid, acc, cnt]
  end
end

sum = 0
cnt = 0
W.times do
  tag, _, acc, c = res.receive
  raise "res" unless tag == :res
  sum += acc
  cnt += c
end
raise "cnt #{cnt}" unless cnt == T
raise "sum" unless sum == (0...T).sum { |x| x * x }
GC.stress = false
raise unless coord.value == T
ws.each(&:value)
puts "OK c29_steal_basic"
