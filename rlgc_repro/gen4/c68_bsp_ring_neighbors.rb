# c68: BSP stencil: each phase x_i' = x_i + left + right using the previous
# phase's full vector broadcast by the coordinator; checked vs simulation.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

N = STRESS ? 3 : 5
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
      raise "tag" unless tag == :val && p2 == ph
      vals[id] = v
    end
    ports.each { |pt| pt << [:vec, ph, vals] }
  end
  :coord_done
end

done = Ractor::Port.new
ws = N.times.map do |i|
  Ractor.new(coord, done, i, P, N) do |c, dp, id, phases, n|
    my = Ractor::Port.new
    c.send([:reg, id, my])
    x = (id * 7 + 2) % 9
    phases.times do |ph|
      c.send([:val, id, ph, x])
      tag, p2, vec = my.receive
      raise "vec" unless tag == :vec && p2 == ph
      raise "self" unless vec[id] == x
      x = x + vec[(id - 1) % n] + vec[(id + 1) % n]
    end
    dp << [:final, id, x]
  end
end

xs = Array.new(N) { |i| (i * 7 + 2) % 9 }
P.times do
  xs = Array.new(N) { |i| xs[i] + xs[(i - 1) % N] + xs[(i + 1) % N] }
end
N.times do
  tag, id, x = done.receive
  raise "final" unless tag == :final && x == xs[id]
end
GC.stress = false
raise unless coord.value == :coord_done
ws.each(&:value)
GC.start
puts "OK c68_bsp_ring_neighbors"
