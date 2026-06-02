% RUBY_RACTOR_LOCAL_GC=1 ruby this.rb   (scenario 5, the strongest of 7; 12/12 default, 6/6 stress, clean)
NW = 16
S3 = Struct.new(:a, :b, :c)
S8 = Struct.new(:a,:b,:c,:d,:e,:f,:g,:h)
D  = Data.define(:x, :y, :z)

def node(depth)
  if depth <= 0
    s = S3.new(1,2,3)
  else
    s = S8.new(node(depth-1), node(depth-1), 0,0,0,0,0, D.new(depth, depth*2, depth*3))
  end
  9.times { |i| s.instance_variable_set("@gv#{i}", "iv#{i}#{depth}") } rescue nil
  s
end

holders = NW.times.map do |hi|
  Ractor.new(hi) do |hi|
    win = []
    loop do
      m = Ractor.receive
      break if m == :stop
      win << m
      win.shift while win.size > 5
      win.each do |bundle|
        h, ks = bundle
        ks.each { |k| h[k] }       # identity lookup by (moved) Struct key address
        h.size
      end
      GC.start(full_mark: true)
    end
    :done
  end
end

hammer  = Thread.new { 500.times { GC.start(full_mark: true); GC.compact } }
hammer2 = Thread.new { 500.times { GC.start(full_mark: false); Thread.pass } }

feeder = Thread.new do
  220.times do |t|
    sub = Ractor.new do
      h = {}.compare_by_identity
      ks = []
      40.times { k = node(2); ks << k; h[k] = ks.size }
      Ractor.make_shareable([h, ks].freeze)   # deep-freeze Struct/Data graph + id-hash, pinned in dying objspace
    end
    bundle = sub.value     # sub dies -> orphan objspace owns pinned id-hash + shareable Struct keys
    holders[t % holders.size].send(bundle) rescue nil
  end
end

feeder.join
hammer.join; hammer2.join
holders.each { |h| h.send(:stop) rescue nil }
holders.each { |h| h.value rescue nil }
puts "OK"