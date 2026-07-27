# Rolling restart: generation N service hands its whole state to main via port
# move on :handoff; main boots generation N+1 seeded with the moved state.
# Axes: 4 generations x 30 ops, move state blob, stress in services.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
done = Ractor::Port.new
boot = lambda do |gen|
  r = Ractor.new(gen, done, STRESS) do |gen, done, stress|
    GC.stress = true if stress
    state = Ractor.receive # seeded state arrives as first message (moved)
    state[:gens] << gen
    loop do
      msg = Ractor.receive
      break if msg == :stop
      op, v, rp = msg
      case op
      when :add
        state[:count] += 1
        state[:sum] += v
        rp << state[:count]
      when :handoff
        GC.stress = false
        rp.send(state, move: true)
        break
      end
    end
    done << :done
    gen
  end
  r
end
rp = Ractor::Port.new
count = 0
sum = 0
state = { count: 0, sum: 0, gens: [] }
svc = boot.call(0)
svc.send(state, move: true)
4.times do |gen|
  30.times do |i|
    v = gen * 100 + i
    count += 1
    sum += v
    svc.send([:add, v, rp])
    raise "g#{gen}i#{i}" unless rp.receive == count
  end
  svc.send([:handoff, nil, rp])
  state = rp.receive
  done.receive
  raise "gen ret" unless svc.value == gen
  raise "carried" unless state[:count] == count && state[:sum] == sum && state[:gens] == (0..gen).to_a
  if gen < 3
    svc = boot.call(gen + 1)
    svc.send(state, move: true)
  end
end
raise "final" unless count == 120 && state[:sum] == (0...4).sum { |g| 30.times.sum { g * 100 + _1 } }
puts "OK d75_rolling_restart_move_state"
