# c71: BSP over a frozen shareable matrix: phase p sums row-range dot phase
# weight; workers own disjoint row ranges; coordinator folds phase totals.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

N = STRESS ? 3 : 4
P = STRESS ? 3 : 5
ROWS = N * (STRESS ? 2 : 5)
MAT = Ractor.make_shareable(Array.new(ROWS) { |r| Array.new(6) { |c| (r * 6 + c) % 13 } })

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
  totals = []
  phases.times do |ph|
    t = 0
    n.times do
      tag, _id, p2, v = (stash.shift || Ractor.receive)
      raise "tag" unless tag == :partial && p2 == ph
      t += v
    end
    totals << t
    ports.each { |pt| pt << [:phase_total, ph, t] }
  end
  totals
end

done = Ractor::Port.new
per = ROWS / N
ws = N.times.map do |i|
  Ractor.new(coord, done, i, P, i * per, per) do |c, dp, id, phases, lo, cnt|
    my = Ractor::Port.new
    c.send([:reg, id, my])
    acc = 0
    phases.times do |ph|
      part = 0
      cnt.times { |k| part += MAT[lo + k].sum * (ph + 1) }
      c.send([:partial, id, ph, part])
      tag, p2, t = my.receive
      raise "tot" unless tag == :phase_total && p2 == ph
      acc += t
    end
    dp << [:final, id, acc]
  end
end

base = MAT.sum(&:sum)
exp_totals = P.times.map { |ph| base * (ph + 1) }
exp_acc = exp_totals.sum
N.times do
  tag, _id, acc = done.receive
  raise "final" unless tag == :final && acc == exp_acc
end
GC.stress = false
raise unless coord.value == exp_totals
ws.each(&:value)
GC.compact unless STRESS
puts "OK c71_bsp_shared_matrix"
