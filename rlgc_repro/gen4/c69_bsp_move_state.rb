# c69: BSP where worker state arrays are moved to the coordinator each phase and
# moved back stamped with the phase aggregate; contents round-trip exactly.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

N = STRESS ? 3 : 4
P = STRESS ? 3 : 5

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
    got = Array.new(n)
    n.times do
      tag, id, p2, st = (stash.shift || Ractor.receive)
      raise "tag" unless tag == :state && p2 == ph
      got[id] = st
    end
    agg = got.sum { |st| st[:acc] }
    n.times do |id|
      st = got[id]
      st[:log] << [ph, agg]
      ports[id].send([:back, ph, agg, st], move: true)
    end
  end
  :coord_done
end

done = Ractor::Port.new
ws = N.times.map do |i|
  Ractor.new(coord, done, i, P) do |c, dp, id, phases|
    my = Ractor::Port.new
    c.send([:reg, id, my])
    st = { id: id, acc: id + 2, log: [] }
    phases.times do |ph|
      c.send([:state, id, ph, st], move: true)
      tag, p2, agg, st2 = my.receive
      raise "back" unless tag == :back && p2 == ph
      st = st2
      raise "identity" unless st[:id] == id
      raise "log" unless st[:log].last == [ph, agg]
      st[:acc] += agg
    end
    dp.send([:final, id, st], move: true)
  end
end

# simulation
accs = Array.new(N) { |i| i + 2 }
logs = Array.new(N) { [] }
P.times do |ph|
  agg = accs.sum
  N.times { |i| logs[i] << [ph, agg]; accs[i] += agg }
end
N.times do
  tag, id, st = done.receive
  raise "final" unless tag == :final
  raise "acc" unless st[:acc] == accs[id]
  raise "logs" unless st[:log] == logs[id]
end
GC.stress = false
raise unless coord.value == :coord_done
ws.each(&:value)
puts "OK c69_bsp_move_state"
