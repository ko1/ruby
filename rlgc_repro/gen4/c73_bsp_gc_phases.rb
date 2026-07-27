# c73: BSP where the coordinator runs GC.start every phase barrier and one
# bounded GC.compact; workers use bounded GC.stress inside their compute step.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false

N = STRESS ? 3 : 4
P = STRESS ? 3 : 5

coord = Ractor.new(N, P, STRESS) do |n, phases, st|
  ports = Array.new(n)
  stash = []
  regs = 0
  while regs < n                    # tolerate early phase-0 values
    msg = Ractor.receive
    if msg[0] == :reg
      ports[msg[1]] = msg[2]
      regs += 1
    else
      stash << msg
    end
  end
  phases.times do |ph|
    s = 0
    n.times do
      tag, _id, p2, v = (stash.shift || Ractor.receive)
      raise "tag" unless tag == :val && p2 == ph
      s += v
    end
    GC.start
    GC.compact if ph == 1 && !st
    ports.each { |pt| pt << [:sum, ph, s] }
  end
  :coord_done
end

done = Ractor::Port.new
ws = N.times.map do |i|
  Ractor.new(coord, done, i, P) do |c, dp, id, phases|
    my = Ractor::Port.new
    c.send([:reg, id, my])
    x = id + 1
    phases.times do |ph|
      GC.stress = true if ENV['S_STRESS']
      tmp = Array.new(12) { |k| (x + k) * (ph + 1) }
      v = tmp.sum
      GC.stress = false
      c.send([:val, id, ph, v])
      tag, p2, s = my.receive
      raise "sum" unless tag == :sum && p2 == ph
      x = (x + s) % 1009
    end
    dp << [:final, id, x]
  end
end

xs = Array.new(N) { |i| i + 1 }
P.times do |ph|
  vs = xs.map { |x| Array.new(12) { |k| (x + k) * (ph + 1) }.sum }
  s = vs.sum
  xs = xs.map { |x| (x + s) % 1009 }
end
N.times do
  tag, id, x = done.receive
  raise "final" unless tag == :final && x == xs[id]
end
raise unless coord.value == :coord_done
ws.each(&:value)
puts "OK c73_bsp_gc_phases"
