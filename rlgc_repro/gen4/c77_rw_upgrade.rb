# c77: upgradeable readers via release-and-reacquire: :upgrade atomically ends
# the read and queues a write; total writes == plain writes + upgrades.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

UP = STRESS ? 2 : 3    # upgrader clients
UO = STRESS ? 2 : 3    # upgrade cycles each
KW = 1                 # plain writer
WO = STRESS ? 2 : 4

coord = Ractor.new(UP + KW) do |nclients|
  version = 0
  readers = 0
  writer = false
  rq = []
  wq = []
  upgrades = 0
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
    when :upgrade                  # read ends; queue as writer
      readers -= 1
      raise "neg" if readers < 0
      upgrades += 1
      if readers == 0 && !writer
        writer = true
        msg[1] << [:wgrant, version]
      else
        wq << msg[1]
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
      if readers == 0 && (w = wq.shift)
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
  raise "final" unless readers == 0 && !writer && rq.empty? && wq.empty?
  [version, upgrades]
end

done = Ractor::Port.new
ups = UP.times.map do |i|
  Ractor.new(coord, done, i, UO) do |c, dp, id, n|
    my = Ractor::Port.new
    last = -1
    n.times do
      c.send([:read, my])
      tag, v = my.receive
      raise "rgrant" unless tag == :rgrant && v >= last
      c.send([:upgrade, my])
      tag2, v2 = my.receive
      raise "up wgrant" unless tag2 == :wgrant && v2 >= v
      last = v2
      c.send([:write_release])
    end
    c.send([:client_done])
    dp << [:udone, id]
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

(UP + KW).times { m = done.receive; raise "done" unless [:udone, :wdone].include?(m[0]) }
GC.stress = false
version, upgrades = coord.value
raise "upgrades" unless upgrades == UP * UO
raise "version" unless version == UP * UO + KW * WO
(ups + ws).each(&:value)
puts "OK c77_rw_upgrade"
