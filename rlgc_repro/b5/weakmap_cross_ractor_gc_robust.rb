## Representative reproducer (S3): the most aggressive concurrent design.
## 18 long-lived Ractors each own a WeakMap + WeakKeyMap whose entries point at
## SHAREABLE values whose HOME objspace is ANOTHER Ractor (ring). Every Ractor
## relentlessly runs lock-free local GC.start + GC.compact, so each weakmap's
## wmap_compact/wkmap_compact calls rb_gc_location() on foreign-home shareables a
## neighbor is concurrently compacting/marking, while main hammers global STW
## GC + compaction. Result: 8/8 default + 6/6 tiny-heap clean, NO crash.
## Run: RUBY_RACTOR_LOCAL_GC=1 /home/ko1/ruby/src/master/ruby this.rb
##  also RUBY_GC_HEAP_INIT_SLOTS=2000 and RUBY_GC_STRESS=1.

N = 18

workers = N.times.map do |i|
  Ractor.new(i) do |id|
    wm  = ObjectSpace::WeakMap.new
    wkm = ObjectSpace::WeakKeyMap.new
    held = []
    rounds = 0
    loop do
      msg = Ractor.receive
      break if msg == :stop
      msg.each_with_index do |v, j|
        k = Ractor.make_shareable("k-#{id}-#{rounds}-#{j}".b)
        wm[k] = v      # weak value: foreign-home shareable
        wkm[k] = v     # strong value: foreign-home shareable (wkmap_mark foreign-skips it)
        held << k if (j & 3) == 0
      end
      held.shift(10) while held.size > 200
      GC.start                 # lock-free local GC (foreign shareables pinned)
      GC.compact rescue nil     # wmap_compact/wkmap_compact -> rb_gc_location on foreign vals
      wm.size
      rounds += 1
    end
    [wm.size, rounds]
  end
end

stop = false
hammer = Thread.new do
  until stop
    GC.start(full_mark: true, immediate_sweep: true)  # global STW unified mark + resolve
    GC.compact rescue nil
  end
end

producer = Thread.new do
  300.times do |round|
    batch = []
    24.times { |j| batch << Ractor.make_shareable("b-#{round}-#{j}-#{rand(1_000_000)}".b) }
    fb = Ractor.make_shareable(batch.freeze)
    workers[round % N].send(fb)   # value produced "elsewhere" -> cross-objspace weak target
  end
  N.times { |i| workers[i].send(:stop) }
end

producer.join
results = workers.map(&:value)
stop = true
hammer.join
GC.start(full_mark: true)
GC.compact rescue nil
puts "done #{results.size} workers"
