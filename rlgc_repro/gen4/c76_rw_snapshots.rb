# c76: RW coordination with data snapshots: readers get frozen copies that must
# be internally consistent ([v, 2v]); writers move delta arrays in; participant stress.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false

MR = STRESS ? 3 : 4
RO = STRESS ? 3 : 5
KW = 2
WO = STRESS ? 2 : 4

coord = Ractor.new(MR + KW) do |nclients|
  data = [0, 0]                  # invariant: data == [v, 2v]
  readers = 0
  writer = false
  rq = []
  wq = []
  dones = 0
  while dones < nclients
    msg = Ractor.receive
    case msg[0]
    when :read
      if !writer && wq.empty?
        readers += 1
        msg[1] << [:snap, data.dup.freeze]
      else
        rq << msg[1]
      end
    when :read_release
      readers -= 1
      raise "neg" if readers < 0
      if readers == 0 && !writer && (w = wq.shift)
        writer = true
        w << [:wgrant, data[0]]
      end
    when :write
      if !writer && readers == 0
        writer = true
        msg[1] << [:wgrant, data[0]]
      else
        wq << msg[1]
      end
    when :write_release
      raise "not writing" unless writer
      delta = msg[1]              # moved array [d]
      d = delta[0]
      data = [data[0] + d, data[1] + 2 * d]
      writer = false
      if (w = wq.shift)
        writer = true
        w << [:wgrant, data[0]]
      else
        until rq.empty?
          readers += 1
          rq.shift << [:snap, data.dup.freeze]
        end
      end
    when :client_done
      dones += 1
    end
    raise "exclusion" if writer && readers > 0
  end
  raise "final" unless readers == 0 && !writer
  data
end

done = Ractor::Port.new
rs = MR.times.map do |i|
  Ractor.new(coord, done, i, RO) do |c, dp, id, n|
    GC.stress = true if ENV['S_STRESS']
    my = Ractor::Port.new
    last = -1
    n.times do
      c.send([:read, my])
      tag, snap = my.receive
      raise "snap" unless tag == :snap && snap.frozen?
      raise "consistency" unless snap[1] == 2 * snap[0]
      raise "monotone" unless snap[0] >= last
      last = snap[0]
      c.send([:read_release])
    end
    GC.stress = false
    c.send([:client_done])
    dp << [:rdone, id]
  end
end
ws = KW.times.map do |i|
  Ractor.new(coord, done, i, WO) do |c, dp, id, n|
    my = Ractor::Port.new
    n.times do
      c.send([:write, my])
      tag, = my.receive
      raise "wgrant" unless tag == :wgrant
      c.send([:write_release, [1]], move: true)
    end
    c.send([:client_done])
    dp << [:wdone, id]
  end
end

(MR + KW).times { m = done.receive; raise "done" unless [:rdone, :wdone].include?(m[0]) }
total = KW * WO
raise "data" unless coord.value == [total, 2 * total]
(rs + ws).each(&:value)
GC.compact unless STRESS
puts "OK c76_rw_snapshots"
