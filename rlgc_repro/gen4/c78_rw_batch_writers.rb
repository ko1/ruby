# c78: RW with batched writes: each write grant commits exactly BATCH increments,
# so every version any reader observes is a multiple of BATCH.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false

BATCH = 3
MR = STRESS ? 3 : 4
RO = STRESS ? 3 : 6
KW = 2
WO = STRESS ? 2 : 4

coord = Ractor.new(MR + KW, BATCH) do |nclients, batch|
  version = 0
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
      raise "batch" unless msg[1] == batch
      version += batch
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

done = Ractor::Port.new
rs = MR.times.map do |i|
  Ractor.new(coord, done, i, RO, BATCH) do |c, dp, id, n, batch|
    GC.stress = true if ENV['S_STRESS']
    my = Ractor::Port.new
    last = -1
    n.times do
      c.send([:read, my])
      tag, v = my.receive
      raise "rgrant" unless tag == :rgrant
      raise "multiple" unless v % batch == 0
      raise "monotone" unless v >= last
      last = v
      c.send([:read_release])
    end
    GC.stress = false
    c.send([:client_done])
    dp << [:rdone, id]
  end
end
ws = KW.times.map do |i|
  Ractor.new(coord, done, i, WO, BATCH) do |c, dp, id, n, batch|
    my = Ractor::Port.new
    n.times do
      c.send([:write, my])
      tag, v = my.receive
      raise "wgrant" unless tag == :wgrant && v % batch == 0
      c.send([:write_release, batch])
    end
    c.send([:client_done])
    dp << [:wdone, id]
  end
end

(MR + KW).times { m = done.receive; raise "done" unless [:rdone, :wdone].include?(m[0]) }
raise "version" unless coord.value == KW * WO * BATCH
(rs + ws).each(&:value)
GC.start
puts "OK c78_rw_batch_writers"
