# c74: readers-writer coordinator, writer priority; exclusion + version
# monotonicity asserted; final version == total writes.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

MR = STRESS ? 3 : 4   # readers
RO = STRESS ? 3 : 6   # reads each
KW = 2                # writers
WO = STRESS ? 2 : 4   # writes each

coord = Ractor.new(MR + KW) do |nclients|
  version = 0
  readers = 0
  writer = false
  rq = []
  wq = []
  max_readers = 0
  dones = 0
  while dones < nclients
    msg = Ractor.receive
    case msg[0]
    when :read
      if !writer && wq.empty?
        readers += 1
        max_readers = readers if readers > max_readers
        msg[1] << [:rgrant, version]
      else
        rq << msg[1]
      end
    when :read_release
      readers -= 1
      raise "neg readers" if readers < 0
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
        max_readers = readers if readers > max_readers
      end
    when :client_done
      dones += 1
    end
    raise "exclusion" if writer && readers > 0
  end
  raise "final state" unless readers == 0 && !writer && rq.empty? && wq.empty?
  [version, max_readers]
end

done = Ractor::Port.new
rs = MR.times.map do |i|
  Ractor.new(coord, done, i, RO) do |c, dp, id, n|
    my = Ractor::Port.new
    last = -1
    n.times do
      c.send([:read, my])
      tag, v = my.receive
      raise "rgrant" unless tag == :rgrant
      raise "monotone" unless v >= last
      last = v
      c.send([:read_release])
    end
    c.send([:client_done])
    dp << [:rdone, id, last]
  end
end
ws = KW.times.map do |i|
  Ractor.new(coord, done, i, WO) do |c, dp, id, n|
    my = Ractor::Port.new
    last = -1
    n.times do
      c.send([:write, my])
      tag, v = my.receive
      raise "wgrant" unless tag == :wgrant
      raise "strict" unless v > last
      last = v
      c.send([:write_release])
    end
    c.send([:client_done])
    dp << [:wdone, id]
  end
end

(MR + KW).times { m = done.receive; raise "done" unless [:rdone, :wdone].include?(m[0]) }
GC.stress = false
version, max_readers = coord.value
raise "version #{version}" unless version == KW * WO
raise "max_readers" unless max_readers >= 1 && max_readers <= MR
(rs + ws).each(&:value)
GC.start
puts "OK c74_rw_writer_priority"
