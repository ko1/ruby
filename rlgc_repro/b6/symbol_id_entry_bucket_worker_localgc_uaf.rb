# Symbol-id-entry bucket Face D: workers are the bucket allocators.
# Each worker interns unique dynamic symbols and promotes each to immortal via rb_sym2id
# (define_method -> set_id_entry). New ID_ENTRY_UNIT(512) buckets (TypedData created at
# symbol.c:280) land in worker objspaces; the worker local minor GC returns from
# rb_gc_mark_roots (gc.c:3444) BEFORE rb_sym_global_symbols_mark_and_move, so it never marks
# ruby_global_symbols.ids/buckets it owns -> sweeps its own bucket still referenced from
# main's ids[] -> later cc-table dup (rb_id_table_foreach -> key2id -> rb_id_serial_to_id)
# reads the freed darray bucket -> SEGV (symbol.c:808/980/997, or writer side :287).
# Run with: RUBY_RACTOR_LOCAL_GC=1  (optionally RUBY_GC_STRESS=1 / RUBY_GC_HEAP_INIT_SLOTS=2000)

N_WORKER = 28
PER      = 9000

workers = N_WORKER.times.map do |w|
  Ractor.new(w) do |w|
    host = Object.new
    sc = host.singleton_class
    PER.times do |j|
      s = "b6q_#{w}_#{j}".to_sym                  # unique mortal dsym in worker objspace
      sc.send(:define_method, s) { 1 } rescue nil # rb_sym2id promotion -> set_id_entry (new bucket)
      host.send(s) rescue nil                      # dispatch -> builds/dups cc table (rb_id_table_foreach)
      s.to_proc rescue nil
      GC.start(full_mark: false, immediate_sweep: true) if (j & 31) == 0  # worker local GC sweeps its bucket
      GC.start if (j & 255) == 0
    end
    w
  end
end

hammer = Thread.new do
  500.times do
    GC.start(full_mark: true, immediate_sweep: true)
    GC.compact rescue nil
  end
end

workers.each(&:take)
hammer.join
puts "done"