# c50: two-level scatter-gather: main -> group leaders -> sub-workers (leaders
# spawn their own ractors and gather on leader-local ports) -> main.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

G = 2                       # leaders
SUB = STRESS ? 2 : 3        # sub-workers per leader
PER = STRESS ? 6 : 20

gather = Ractor::Port.new
leaders = G.times.map do |gi|
  Ractor.new(gather, gi, SUB) do |g, gid, nsub|
    tag, chunk = Ractor.receive
    raise "chunk" unless tag == :chunk
    lp = Ractor::Port.new
    subs = nsub.times.map do |si|
      slice = chunk.each_slice((chunk.size + nsub - 1) / nsub).to_a[si] || []
      Ractor.new(lp, si, slice) do |p, sid, sl|
        p << [:sub, sid, sl.sum, sl.size]
        :sub_done
      end
    end
    total = 0
    cnt = 0
    nsub.times do
      t, _sid, s, n = lp.receive
      raise "sub" unless t == :sub
      total += s
      cnt += n
    end
    raise "cnt" unless cnt == chunk.size
    subs.each { |s| raise unless s.value == :sub_done }
    g << [:group, gid, total]
    :leader_done
  end
end

data = (0...(G * PER)).map { |i| (i * 7) % 31 }
G.times { |gi| leaders[gi].send([:chunk, data[gi * PER, PER]]) }

total = 0
G.times do
  tag, _gid, s = gather.receive
  raise "group" unless tag == :group
  total += s
end
raise "total" unless total == data.sum
GC.stress = false
leaders.each { |l| raise unless l.value == :leader_done }
puts "OK c50_scatter_two_level"
