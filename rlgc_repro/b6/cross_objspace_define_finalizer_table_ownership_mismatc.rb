# Minimal deterministic [BUG] on ractor-local-gc (15/15 default, 10/10 GC_STRESS, 10/10 tiny heap).
# Run: RUBY_RACTOR_LOCAL_GC=1 ruby this.rb
#
# A worker registers a finalizer on a Class OWNED BY MAIN, then drops it. The finalizer
# entry lands in the WORKER's finalizer_table (define routes by rb_gc_get_objspace() =
# current Ractor), while FL_FINALIZE is set on the MAIN-owned class. The finalizer-table
# KEY is not a GC root, so main eventually sweeps the class; run_final searches MAIN's
# table, finds nothing, and rb_bug()s:
#   [BUG] FL_FINALIZE flag is set, but finalizers are not found  (default.c:3417)
# No compaction, no worker GC, no worker termination required.
req = Ractor.new do
  loop do
    c = Ractor.receive
    break if c == :stop
    ObjectSpace.define_finalizer(c, proc { |id| })  # entry -> worker table; flag -> main object
    c = nil
  end
end
500.times { req.send(Class.new) }   # anonymous classes created & OWNED by MAIN
req.send(:stop); req.value
20.times { GC.start(full_mark: true, immediate_sweep: true) }
puts "OK"
