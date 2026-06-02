# Concurrent const_set + remove_const on the SAME shareable modules from many
# Ractors. rb_const_remove (variable.c:3649) is NOT under RB_VM_LOCKING, unlike
# const_set / const_lookup, so concurrent removes double-free the rb_const_entry_t
# (and a remove racing a const_set writes into freed memory).
# Crash: "double free or corruption (fasttop)" SIGABRT in rb_const_remove -> SIZED_FREE
# (variable.c:3685). Reproduces 3/3 on default heap, RUBY_GC_STRESS=1, and tiny heap.
NMOD = 6
NWORK = 24   # > typical core count

LOADP = []
NMOD.times do |i|
  path = "/tmp/claude-1000/fl_#{i}.rb"
  File.write(path, "Fod#{i}.const_set(:AUTO, #{i}.freeze)\n")
  LOADP << path
end

mods = []
NMOD.times do |i|
  m = Module.new
  Object.const_set("Fod#{i}", m)
  m.autoload(:AUTO, LOADP[i])
  Ractor.make_shareable(m)   # shareable module; NOT frozen
  mods << m
end
MODS = Ractor.make_shareable(mods)

# hammer thread: keep GC running constantly
hg = Thread.new { 6000.times { GC.start(full_mark: false); Thread.pass } }

workers = NWORK.times.map do |wid|
  Ractor.new(wid, MODS) do |wid, mods|
    2000.times do |iter|
      m = mods[(wid + iter) % mods.size]
      k = :"K#{(wid * 3 + iter) % 6}"
      # all workers churn the SAME shared const table concurrently
      begin; m.const_set(k, (wid * 1000 + iter).freeze); rescue; end
      begin; m.const_get(k, false); rescue; end
      begin; m.send(:remove_const, k) if m.const_defined?(k, false); rescue; end
      begin; m::AUTO; rescue; end   # autoload resolution (routes cross-ractor to main)
      GC.start if iter % 19 == 0    # frequent GC in workers
    end
    :done
  end
end

workers.each(&:value)
hg.join
puts "ok"
