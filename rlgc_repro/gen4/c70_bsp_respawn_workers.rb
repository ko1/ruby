# c70: BSP with per-phase worker respawn: each phase spawns fresh workers that
# fold the previous phase's states (passed as args) and report; no persistent ractors.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false

N = STRESS ? 3 : 4
P = STRESS ? 3 : 5

states = Array.new(N) { |i| i * 3 + 1 }
P.times do |ph|
  GC.stress = true if STRESS && ph == 0
  gather = Ractor::Port.new
  s = states.sum
  ws = N.times.map do |i|
    Ractor.new(gather, i, states[i], s, ph) do |g, id, x, total, phase|
      GC.stress = true if ENV['S_STRESS'] && phase > 0
      nx = x * 2 + total % 7
      GC.stress = false
      g << [:next, id, nx]
      nx
    end
  end
  nstates = Array.new(N)
  N.times do
    tag, id, nx = gather.receive
    raise "tag" unless tag == :next
    nstates[id] = nx
  end
  GC.stress = false
  ws.each_with_index { |w, i| raise "val" unless w.value == nstates[i] }
  states = nstates
  GC.start if ph == 1 && !STRESS
end

sim = Array.new(N) { |i| i * 3 + 1 }
P.times do
  s = sim.sum
  sim = sim.map { |x| x * 2 + s % 7 }
end
raise "states #{states.inspect}" unless states == sim
puts "OK c70_bsp_respawn_workers"
