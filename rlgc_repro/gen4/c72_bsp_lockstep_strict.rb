# c72: BSP lockstep audit: coordinator keeps a strict per-phase message counter
# and raises on any phase-(p+1) value arriving before phase p closes; jittered work.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

N = STRESS ? 4 : 5
P = STRESS ? 3 : 8

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
  current = 0
  seen = 0
  sums = Array.new(phases, 0)
  (n * phases).times do
    tag, _id, ph, v = (stash.shift || Ractor.receive)
    raise "tag" unless tag == :val
    raise "early phase #{ph} (current #{current}, seen #{seen})" unless ph == current
    sums[ph] += v
    seen += 1
    if seen == n
      ports.each { |pt| pt << [:go, current, sums[current]] }
      current += 1
      seen = 0
    end
  end
  sums
end

done = Ractor::Port.new
ws = N.times.map do |i|
  Ractor.new(coord, done, i, P) do |c, dp, id, phases|
    my = Ractor::Port.new
    c.send([:reg, id, my])
    acc = 0
    phases.times do |ph|
      # jitter: variable amount of allocation before submitting
      junk = Array.new((id * 7 + ph * 11) % 23 + 1) { |k| k.to_s }
      v = id * 100 + ph + junk.size
      c.send([:val, id, ph, v])
      tag, p2, s = my.receive
      raise "go" unless tag == :go && p2 == ph
      acc += s
    end
    dp << [:final, id, acc]
  end
end

exp_sums = P.times.map do |ph|
  N.times.sum { |id| id * 100 + ph + ((id * 7 + ph * 11) % 23 + 1) }
end
exp_acc = exp_sums.sum
N.times do
  tag, _id, acc = done.receive
  raise "final" unless tag == :final && acc == exp_acc
end
GC.stress = false
raise unless coord.value == exp_sums
ws.each(&:value)
puts "OK c72_bsp_lockstep_strict"
