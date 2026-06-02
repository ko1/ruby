GC.auto_compact = true
NW = 18

holders = (0...8).map do
  Ractor.new do
    store = []
    loop do
      m = Ractor.receive
      break if m == :stop
      store << m
      store.shift while store.size > 250
    end
    store.size
  end
end

hammer = Thread.new do
  500.times { GC.start(full_mark: true, immediate_sweep: true) }
end

workers = (0...NW).map do |wi|
  Ractor.new(wi, holders) do |wi, holders|
    300.times do |i|
      host = (i % 2 == 0) ? (+"str#{wi}_#{i}") : [wi, i, i*3]
      8.times { |k| host.instance_variable_set("@iv#{k}", [k, +"v#{k}"]) }
      if i.even?
        sh = (i % 2 == 0) ? (+"sh#{wi}_#{i}") : [1, 2, 3]
        8.times { |k| sh.instance_variable_set("@s#{k}", k) }
        begin
          Ractor.make_shareable(sh)
          holders[(wi + i) % holders.size].send(sh)
        rescue
        end
      end
      GC.start(full_mark: true) if i % 15 == 0
    end
    :done
  end
end

workers.each(&:value)
hammer.join
holders.each { |h| h.send(:stop) }
holders.each(&:value)
puts "OK"