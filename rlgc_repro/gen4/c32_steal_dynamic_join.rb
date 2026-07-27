# c32: work stealing where E extra workers join mid-run (main spawns them when
# coordinator reports half the tasks handed); totals still conserved.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

W = STRESS ? 2 : 3
E = 2
T = STRESS ? 8 : 30

prog = Ractor::Port.new
coord = Ractor.new(W + E, T, prog) do |wtotal, t, pp|
  queues = Array.new(wtotal) { [] }
  t.times { |x| queues[x % 2] << x }   # only first two queues seeded
  handed = 0
  dones = 0
  reported = false
  while dones < wtotal
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
      if handed >= t / 2 && !reported
        pp << [:progress, handed]
        reported = true
      end
    else
      dones += 1
      reply << [:done]
    end
  end
  raise "handed" unless handed == t
  raise "report" unless reported
  handed
end

res = Ractor::Port.new
mk = lambda do |i|
  Ractor.new(coord, res, i) do |c, rp, wid|
    my = Ractor::Port.new
    acc = 0
    cnt = 0
    loop do
      c.send([:get, wid, my])
      msg = my.receive
      break if msg[0] == :done
      acc += msg[1]
      cnt += 1
    end
    rp << [:res, wid, acc, cnt]
  end
end
ws = W.times.map { |i| mk.call(i) }
tag, h = prog.receive
raise "prog" unless tag == :progress && h >= T / 2
ws += E.times.map { |j| mk.call(W + j) }

sum = 0
cnt = 0
(W + E).times do
  t2, _, acc, c = res.receive
  raise "res" unless t2 == :res
  sum += acc; cnt += c
end
raise "cnt" unless cnt == T
raise "sum" unless sum == (0...T).sum
GC.stress = false
raise unless coord.value == T
ws.each(&:value)
puts "OK c32_steal_dynamic_join"
