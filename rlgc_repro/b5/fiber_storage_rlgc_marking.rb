# Most aggressive scenario (s7): 20 Ractors relentlessly create/suspend/KILL fibers
# holding storage graphs, each running lock-free local GC.start, while 3 hammer threads
# drive STW global full GCs. Fiber#kill runs ensure-blocks that allocate during teardown
# with live storage present. NO CRASH across 0/12+0/20 default, 0/6 stress, 0/6 tiny-heap.
# (Run: RUBY_RACTOR_LOCAL_GC=1 ruby this.rb)
NR = 20
RND = 60

hammers = 3.times.map do
  Thread.new do
    800.times do
      GC.start(full_mark: true, immediate_sweep: true)
      Thread.pass
    end
  end
end

ractors = NR.times.map do |ri|
  Ractor.new(ri) do |ri|
    RND.times do |r|
      fibers = 12.times.map do |fi|
        Fiber.new(storage: {tag: "#{ri}.#{fi}.#{r}".dup}) do
          begin
            Fiber[:g] = (0..12).map { |i| ["v#{i}".dup, {n: i}] }
            Fiber[:t] = [[1,[2,[3]]], {a: [4,5,6]}]
            Fiber.yield
            Fiber[:g2] = "x" * 100
            Fiber.yield
            Fiber[:g].size
          ensure
            tmp = (0..8).map { |i| "ens#{i}".dup }  # allocate during kill/teardown
            tmp.size
          end
        end
      end
      fibers.each(&:resume)        # suspend all at first yield (live storage)
      GC.start                     # local GC: suspended fibers' storage must survive
      fibers.each_with_index do |f, i|
        if i.even?
          f.kill                   # terminate with live storage -> ensure allocates
        else
          f.resume
        end
      end
      GC.start(full_mark: true)
      fibers = nil                 # drop -> remaining become garbage w/ live storage
      GC.start if r % 2 == 0
    end
    :done
  end
end

ractors.each(&:value)
hammers.each(&:join)
puts "OK"
