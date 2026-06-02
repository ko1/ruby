# sh3: too_complex (st-based ivars) shareable objects born in ephemeral/orphan
# objspaces, fanned to long-lived holders, hammered with compact + full GC.
# Representative of the assigned shape-tree x cross-Ractor x compaction probe.
# RUN: RUBY_RACTOR_LOCAL_GC=1 ruby this.rb  (also tiny-heap + GC_STRESS=1). 0 crashes.
NHOLD = 8
holders = (0...NHOLD).map do
  Ractor.new do
    store = []
    loop do
      m = Ractor.receive
      break if m == :done
      store << m
      store.shift while store.size > 600
      store.last(40).each do |o|
        o.instance_variables.each { |iv| o.instance_variable_get(iv) }
      end
    end
    store.size
  end
end

hammer = Thread.new do
  600.times do
    GC.start(full_mark: true, immediate_sweep: true)
    GC.compact rescue nil
  end
end

GROUPS = 50
PERG = 16
GROUPS.times do |g|
  prod = (0...PERG).map do |j|
    Ractor.new(g, j, holders) do |g, j, holders|
      k = Class.new
      made = []
      6.times do |t|
        o = k.new
        n = 260 + ((g + j + t) % 40)        # >256 ivars -> too_complex st-based storage
        n.times { |i| o.instance_variable_set("@x#{(i * 7 + t) % n}", i) }
        s = Ractor.make_shareable(o)
        holders[(g + j + t) % holders.size].send(s)
        made << s
      end
      made.size
    end
  end
  prod.each { |r| r.value rescue nil }       # producers die here -> orphan objspaces
  GC.start(full_mark: true) if (g & 1) == 0
end

GC.compact rescue nil
GC.start(full_mark: true)
holders.each { |h| h.send(:done) }
holders.each { |h| h.value rescue nil }
hammer.join
puts "ok"
