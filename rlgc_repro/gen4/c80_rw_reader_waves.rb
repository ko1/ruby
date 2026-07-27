# c80: persistent RW coordinator + writers; readers arrive in respawned waves
# (joined between waves); coordinator runs bounded GC.compact during service.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false

WAVES = STRESS ? 2 : 3
MR = STRESS ? 2 : 3    # readers per wave
RO = 3
KW = 1
WO = STRESS ? 2 : 4

NCLIENTS = WAVES * MR + KW
coord = Ractor.new(NCLIENTS, STRESS) do |nc, st|
  version = 0
  readers = 0
  writer = false
  rq = []
  wq = []
  dones = 0
  msgs = 0
  compacts = 0
  while dones < nc
    msg = Ractor.receive
    msgs += 1
    if msgs % 20 == 0 && compacts < 2 && !st
      GC.compact
      compacts += 1
    end
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

done = Ractor::Port.new
WDONES = [0]
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

WAVES.times do |wv|
  GC.stress = true if STRESS && wv == 0
  wave = MR.times.map do |i|
    Ractor.new(coord, done, wv * 100 + i, RO, wv) do |c, dp, id, n, wave2|
      GC.stress = true if ENV['S_STRESS'] && wave2 > 0
      my = Ractor::Port.new
      last = -1
      n.times do
        c.send([:read, my])
        tag, v = my.receive
        raise "rgrant" unless tag == :rgrant && v >= last
        last = v
        c.send([:read_release])
      end
      GC.stress = false
      c.send([:client_done])
      dp << [:rdone, id]
    end
  end
  got = 0
  while got < MR
    m = done.receive
    case m[0]
    when :rdone then got += 1
    when :wdone then WDONES[0] += 1    # persistent writer may finish mid-wave
    else raise "wave done"
    end
  end
  GC.stress = false
  wave.each(&:value)
end
(KW - WDONES[0]).times { m = done.receive; raise "wdone" unless m[0] == :wdone }
raise "version" unless coord.value == KW * WO
ws.each(&:value)
GC.start
puts "OK c80_rw_reader_waves"
