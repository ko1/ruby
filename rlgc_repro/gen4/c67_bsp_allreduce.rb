# c67: BSP allreduce: P phases of x_i' = x_i + sum(all x); coordinator gathers,
# broadcasts the phase sum; result checked against a sequential simulation.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

N = STRESS ? 3 : 4
P = STRESS ? 3 : 6

coord = Ractor.new(N, P) do |n, phases|
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
    vals = Array.new(n)
    n.times do
      tag, id, p2, v = (stash.shift || Ractor.receive)
      raise "phase tag" unless tag == :val
      raise "lockstep #{p2} != #{ph}" unless p2 == ph
      raise "dup" if vals[id]
      vals[id] = v
    end
    s = vals.sum
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
      c.send([:val, id, ph, x])
      tag, p2, s = my.receive
      raise "sum" unless tag == :sum && p2 == ph
      x += s
    end
    dp << [:final, id, x]
  end
end

# sequential simulation
xs = Array.new(N) { |i| i + 1 }
P.times do
  s = xs.sum
  xs = xs.map { |x| x + s }
end
N.times do
  tag, id, x = done.receive
  raise "final" unless tag == :final
  raise "x[#{id}] #{x} != #{xs[id]}" unless x == xs[id]
end
GC.stress = false
raise unless coord.value == :coord_done
ws.each(&:value)
puts "OK c67_bsp_allreduce"
