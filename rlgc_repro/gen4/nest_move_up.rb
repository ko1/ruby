# gen4 nested: leaves build result buffers that are MOVED up the chain:
# leaf -> supervisor (via port move) -> main (via port move). Two levels of
# move across three ractor generations.
# axes: transfer=move upward, GC=GC.start in supervisor after relay, depth=2
N_SUP = 3
N_LEAF = 3

up = Ractor::Port.new
sups = N_SUP.times.map do |sid|
  Ractor.new(up, sid, N_LEAF) do |top, sup_id, nleaf|
    gather = Ractor::Port.new
    leaves = nleaf.times.map do |lid|
      Ractor.new(gather, sup_id, lid) do |g, s, l|
        buf = "leaf-#{s}-#{l}:" + ("d" * (20 + s * 10 + l))
        g.send(buf, move: true)
        :leaf_done
      end
    end
    nleaf.times do
      buf = gather.receive
      buf << "|relayed#{sup_id}"
      top.send(buf, move: true)
      GC.start
    end
    leaves.each(&:join)
    :sup_done
  end
end

exp_lens = {}
N_SUP.times do |s|
  N_LEAF.times do |l|
    key = "leaf-#{s}-#{l}"
    exp_lens[key] = "#{key}:".size + 20 + s * 10 + l + "|relayed#{s}".size
  end
end

got = {}
(N_SUP * N_LEAF).times do
  buf = up.receive
  key = buf.split(":").first
  raise "FAIL relay marker" unless buf.include?("|relayed")
  got[key] = buf.size
end
sups.each { |s| raise "FAIL sup" unless s.value == :sup_done }
raise "FAIL lens" unless got == exp_lens
puts "OK nest_move_up"
