# c53: gather via a reduce tree: 4 leaves -> 2 combiners -> root -> main; every
# node registers its input port through main, which wires the topology.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

PER = STRESS ? 8 : 30
final = Ractor::Port.new
reg = Ractor::Port.new

root = Ractor.new(reg, final) do |rp, fin|
  my = Ractor::Port.new
  rp << [:root, my]
  a = my.receive
  b = my.receive
  raise "root tags" unless a[0] == :acc && b[0] == :acc
  fin << [:total, a[1] + b[1], a[2] + b[2]]
  :root_done
end
tagr, root_port = reg.receive
raise unless tagr == :root

combiners = 2.times.map do |ci|
  Ractor.new(reg, root_port, ci) do |rp, up, cid|
    my = Ractor::Port.new
    rp << [:comb, cid, my]
    a = my.receive
    b = my.receive
    raise "comb tags" unless a[0] == :leaf && b[0] == :leaf
    up << [:acc, a[1] + b[1], a[2] + b[2]]
    :comb_done
  end
end
comb_ports = Array.new(2)
2.times do
  t, cid, port = reg.receive
  raise unless t == :comb
  comb_ports[cid] = port
end

data = (0...(4 * PER)).map { |i| (i * 3) % 17 }
leaves = 4.times.map do |li|
  Ractor.new(comb_ports[li / 2], li, data[li * PER, PER]) do |up, _lid, chunk|
    up << [:leaf, chunk.sum, chunk.size]
    :leaf_done
  end
end

tag, total, cnt = final.receive
raise "final" unless tag == :total
raise "cnt" unless cnt == 4 * PER
raise "total" unless total == data.sum
GC.stress = false
raise unless root.value == :root_done
combiners.each { |c| raise unless c.value == :comb_done }
leaves.each { |l| raise unless l.value == :leaf_done }
puts "OK c53_scatter_reduce_tree"
