$VERBOSE = nil
# Strongest scenario (const-cache lock-free-removal race). Structural hazard
# confirmed in code (remove_from_constant_cache mutates VM-global vm->constant_cache
# with NO VM lock during a Ractor's lock-free local GC sweep, racing VM-lock-held
# vm_track_constant_cache inserts + rb_clear_constant_cache_for_id invalidation),
# but did NOT tear into a crash across ~150 runs (snapshot stress can't catch it).

NW = 22
K = Class.new
K.const_set(:V, [0].freeze)
Ractor.make_shareable(K)
Ractor.make_shareable(K::V)

ws = NW.times.map do |i|
  Ractor.new(i) do |i|
    Ractor.receive
    80.times do
      live = []
      200.times do |k|
        pr = eval("proc { K::V.length + #{k} }")
        pr.call            # IC miss -> tracked into vm->constant_cache[:V] (lock)
        live << pr
      end
      live = nil
      # mass lock-free local sweep -> remove_from_constant_cache x200 (NO lock)
      GC.start(full_mark: false, immediate_sweep: true)
    end
    :ok
  end
end
ws.each { |w| w.send(:go) }

# main: invalidation foreach (writes into each tracked IC) racing the lock-free
# deletes, plus an insert storm on the same set_table.
inval = Thread.new do
  v = 0
  100000.times do
    v += 1
    nv = [v].freeze; Ractor.make_shareable(nv)
    K.const_set(:V, nv)   # rb_clear_constant_cache_for_id(:V): set_table_foreach
  end
end
500.times { 300.times { |j| pr = eval("proc { K::V.size + #{j} }"); pr.call } }

ws.each(&:value)
inval.join
puts "done"