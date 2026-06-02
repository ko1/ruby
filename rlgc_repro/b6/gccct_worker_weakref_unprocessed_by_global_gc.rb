ERR_FALSE_POSITIVE_NO_CRASH = true
# Negative result: no crash reproduced on the HEAD binary. Below is the strongest probe
# (s6) that exercises the identified latent bug (worker-owned gccct cc whose weak klass/
# cme dies in a global GC while the worker objspace's weak_references darray is never
# processed by the global GC). It runs clean ("done") -- documented robust, not crashing.
NW = 16
NC = 6
NM = 12
defs = (0...NM).map { |i| "def m#{i}; #{i}; end" }.join(";")
DEFS = Ractor.make_shareable(defs)
MIDS = Ractor.make_shareable((0...NM).map { |i| :"m#{i}" })

workers = NW.times.map do |w|
  Ractor.new(DEFS, MIDS, NC, w) do |defs, mids, nc, wid|
    30.times do
      # ALL class_eval up front (each clears the whole gccct -- fine), then a FINAL
      # send-burst with NO def after, parking worker-owned ccs (klass/cme = these anon
      # classes, allocated in THIS worker's objspace) into the VM-global gccct.
      classes = nc.times.map { k = Class.new; k.class_eval(defs); k }
      objs = classes.map(&:new)
      objs.each { |o| mids.each { |m| o.send(m) } }
      classes = nil; objs = nil   # drop -> anon classes/cmes become collectible
      # No def from here: gccct is NOT cleared. Local GCs pin; global GCs (from main)
      # reclaim the dead anon classes/cmes -> the worker ccs go dangling. A worker's OWN
      # next local minor GC re-validates them (self-heal) -- which is why no crash.
      8.times do
        GC.start(full_mark: false)
        1000.send(:to_s); "abc".send(:upcase); [1].send(:first); {a:1}.send(:size)
      end
    end
    wid
  end
end

stop = false
hammer = Thread.new do
  i = 0
  until stop
    GC.start(full_mark: true, immediate_sweep: true)  # GLOBAL GC: rb_vm_mark reads gccct
    GC.compact if (i % 6) == 0
    i += 1
  end
end

workers.each(&:value)
stop = true
hammer.join
puts "done"