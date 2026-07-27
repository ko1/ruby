# c13: hierarchical (tree) barrier: 2 leaf barriers of G participants each,
# leaves synchronize through a root barrier; lockstep asserted at every level.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

G = STRESS ? 2 : 3       # per-group participants
R = STRESS ? 3 : 8

root = Ractor.new(2, R) do |n, rmax|
  rmax.times do |round|
    ports = Array.new(n)
    n.times do
      tag, gid, rd, port = Ractor.receive
      raise "root tag" unless tag == :arrive && rd == round
      ports[gid] = port
    end
    ports.each { |p| p << [:go, round] }
  end
  :root_done
end

leaves = 2.times.map do |gid|
  Ractor.new(root, G, R, gid) do |rt, n, rmax, g|
    my = Ractor::Port.new
    rmax.times do |round|
      ports = []
      n.times do
        tag, _id, rd, port = Ractor.receive
        raise "leaf tag" unless tag == :arrive && rd == round
        ports << port
      end
      rt.send([:arrive, g, round, my])
      tag, rd = my.receive
      raise "leaf go" unless tag == :go && rd == round
      ports.each { |p| p << [:go, round] }
    end
    :leaf_done
  end
end

done = Ractor::Port.new
ws = []
2.times do |gid|
  G.times do |i|
    ws << Ractor.new(leaves[gid], done, gid * 10 + i, R) do |lf, dp, id, rmax|
      my = Ractor::Port.new
      rmax.times do |round|
        lf.send([:arrive, id, round, my])
        tag, rd = my.receive
        raise "w go" unless tag == :go && rd == round
      end
      dp << [:done, id]
    end
  end
end

(2 * G).times { t, = done.receive; raise "done" unless t == :done }
GC.stress = false
raise unless root.value == :root_done
leaves.each { |l| raise unless l.value == :leaf_done }
ws.each(&:value)
puts "OK c13_barrier_tree"
