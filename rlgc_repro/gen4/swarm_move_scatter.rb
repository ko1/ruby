# main が多数 Ractor へ move で散布し回収する
# axes: many-ractor, move, scatter-gather
Warning[:experimental] = false
N = ENV['S_STRESS'] ? 24 : 96
GC.stress = true if ENV['S_STRESS']
rs = Array.new(N) { Ractor.new { m = Ractor.receive; m[:arr].sum + m[:str].size } }
rs.each_with_index { |r, i| r.send({ arr: [i, i+1], str: +"mv#{i}" }, move: true) }
GC.start
vals = rs.map(&:value)
raise unless vals.each_with_index.all? { |v, i| v == i + (i+1) + "mv#{i}".size }
puts "OK swarm_move_scatter"
