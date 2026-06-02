# RLGC Face B: rb_const_remove invalidates the VM-global constant cache WITHOUT the VM
# lock (variable.c:3675 -> rb_clear_constant_cache_for_id -> set_table_foreach over the
# per-id set_table), while non-main Ractors concurrently MISS the cache on the same id
# and insert into that same set_table UNDER the VM lock (vm_track_constant_cache,
# RB_VM_LOCKING). Because remove_const does not hold that lock, a concurrent set_insert
# rehash reallocates the table's entries[] array out from under the foreach walk => a
# garbage IC pointer => SEGV writing ((IC)ic)->entry = NULL at vm_method.c:323.
# Run: RUBY_RACTOR_LOCAL_GC=1 ruby this.rb   (crashes ~1/10-1/30; loop it)
class Cfg
  HOT = [0].freeze    # single hot constant we churn + miss on
end

NR = 18
ractors = NR.times.map do
  Ractor.new do
    1000.times do
      # fresh iseq each call => fresh IC => set_insert(ics_for_HOT, ic) under VM lock,
      # forcing the set_table to grow/rehash while main walks it unlocked.
      eval("proc { ::Cfg::HOT }").call rescue nil
    end
    :ok
  end
end

# short-lived killer Ractors: seed an IC then die immediately (orphan objspace)
killers = 80.times.map do
  Ractor.new { eval("proc { ::Cfg::HOT }").call rescue nil; :x }
end

# Main: tight BARE remove_const + const_set of HOT (the UNLOCKED clear walk).
2000.times do |k|
  begin
    Cfg.send(:remove_const, :HOT) if Cfg.const_defined?(:HOT, false)
  rescue
  end
  begin
    Cfg.const_set(:HOT, [k].freeze)
  rescue
  end
  GC.start if k % 50 == 0
end

ractors.each { |r| r.value rescue nil }
killers.each { |r| r.value rescue nil }
puts "OK"