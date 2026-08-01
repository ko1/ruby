# c75: readers-writer with reader priority (reads pass queued writers); scripts
# are finite so writers still finish; final version == total writes.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

MR = STRESS ? 3 : 4
RO = STRESS ? 3 : 6
KW = 2
WO = STRESS ? 2 : 4

coord = Ractor.new(MR + KW) do |nclients|
  version = 0
  readers = 0
  writer = false
  rq = []
  wq = []
  dones = 0
  grants_r = 0
  while dones < nclients
    msg = Ractor.receive
    case msg[0]
    when :read
      if !writer
        readers += 1
        grants_r += 1
        msg[1] << [:rgrant, version]
      else
        # reader priority: readers queue only while a writer is active
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
      if rq.empty?
        if (w = wq.shift)
          writer = true
          w << [:wgrant, version]
        end
      else
        rq.each do |p|
          readers += 1
          grants_r += 1
          p << [:rgrant, version]
        end
        rq = []
      end
    when :client_done
      dones += 1
    end
    raise "exclusion" if writer && readers > 0
  end
  raise "final" unless readers == 0 && !writer && wq.empty? && rq.empty?
  [version, grants_r]
end

done = Ractor::Port.new
rs = MR.times.map do |i|
  Ractor.new(coord, done, i, RO) do |c, dp, id, n|
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
  Ractor.new(coord, done, i, WO) do |c, dp, id, n|
    my = Ractor::Port.new
    last = -1
    n.times do
      c.send([:write, my])
      tag, v = my.receive
      raise "wgrant" unless tag == :wgrant && v > last
      last = v
      c.send([:write_release])
    end
    c.send([:client_done])
    dp << [:wdone, id]
  end
end

(MR + KW).times { m = done.receive; raise "done" unless [:rdone, :wdone].include?(m[0]) }
GC.stress = false
version, grants_r = coord.value
raise "version" unless version == KW * WO
raise "grants_r" unless grants_r == MR * RO
(rs + ws).each(&:value)
puts "OK c75_rw_reader_priority"
