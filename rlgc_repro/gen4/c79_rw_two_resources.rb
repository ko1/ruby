# c79: two RW coordinators (A, B) with global acquisition order A->B for writers
# (deadlock-free); readers touch a single resource; both versions asserted.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

MR = STRESS ? 2 : 4    # readers (half on A, half on B)
RO = STRESS ? 3 : 5
KW = 2                 # writers over both
WO = STRESS ? 2 : 3

def make_rw(nclients)
  Ractor.new(nclients) do |nc|
    version = 0
    readers = 0
    writer = false
    rq = []
    wq = []
    dones = 0
    while dones < nc
      msg = Ractor.receive
      case msg[0]
      when :read
        if !writer && wq.empty?
          readers += 1
          msg[1] << [:rgrant, version]
        else
          rq << msg[1]
        end
      when :read_release
        readers -= 1
        raise "neg" if readers < 0
        if readers == 0 && !writer && (w = wq.shift)
          writer = true
          w << [:wgrant, version]
        end
      when :write
        if !writer && readers == 0
          writer = true
          msg[1] << [:wgrant, version]
        else
          wq << msg[1]
        end
      when :write_release
        raise "not writing" unless writer
        version += 1
        writer = false
        if (w = wq.shift)
          writer = true
          w << [:wgrant, version]
        else
          until rq.empty?
            readers += 1
            rq.shift << [:rgrant, version]
          end
        end
      when :client_done
        dones += 1
      end
      raise "exclusion" if writer && readers > 0
    end
    version
  end
end

half = MR / 2
ca = make_rw(half + KW)
cb = make_rw((MR - half) + KW)

done = Ractor::Port.new
rs = MR.times.map do |i|
  target = i < half ? ca : cb
  Ractor.new(target, done, i, RO) do |c, dp, id, n|
    my = Ractor::Port.new
    last = -1
    n.times do
      c.send([:read, my])
      tag, v = my.receive
      raise "rgrant" unless tag == :rgrant && v >= last
      last = v
      c.send([:read_release])
    end
    c.send([:client_done])
    dp << [:rdone, id]
  end
end
ws = KW.times.map do |i|
  Ractor.new(ca, cb, done, i, WO) do |a, b, dp, id, n|
    my = Ractor::Port.new
    n.times do
      a.send([:write, my])
      tag, = my.receive
      raise "a wgrant" unless tag == :wgrant
      b.send([:write, my])
      tag2, = my.receive
      raise "b wgrant" unless tag2 == :wgrant
      b.send([:write_release])
      a.send([:write_release])
    end
    a.send([:client_done])
    b.send([:client_done])
    dp << [:wdone, id]
  end
end

(MR + KW).times { m = done.receive; raise "done" unless [:rdone, :wdone].include?(m[0]) }
GC.stress = false
raise "va" unless ca.value == KW * WO
raise "vb" unless cb.value == KW * WO
(rs + ws).each(&:value)
puts "OK c79_rw_two_resources"
