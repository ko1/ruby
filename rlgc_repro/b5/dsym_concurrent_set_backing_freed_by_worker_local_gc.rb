RN = 20

# Short-lived "builder" Ractors intern a huge number of DISTINCT dynamic symbols.
# That forces ruby_global_symbols.sym_set (a non-shareable, non-WB-protected,
# non-rooted concurrent_set object) to RESIZE -- the new backing object is
# allocated in the BUILDER's own objspace. The builder then dies. A worker LOCAL
# minor GC (its own GC.start, or another worker's) does NOT mark global_symbols
# (gc.c rb_gc_mark_roots local branch returns before the global_symbols mark) and
# the object is unshareable (so the shareable sweep-pin does not save it), so it is
# swept and concurrent_set_free releases its entries[]. ruby_global_symbols.sym_set
# now dangles -> the next "x".to_sym in ANY Ractor faults in rb_concurrent_set_find.
30.times do |round|
  builder = Ractor.new(round) do |round|
    40000.times { |i| _ = "r#{round}_grow_#{i}".to_sym }  # forces table doublings
    GC.start
    :built
  end

  others = (0...8).map do |w|
    Ractor.new(round, w) do |round, w|
      8000.times do |i|
        _ = "r#{round}_w#{w}_#{i}".to_sym
        _ = "r#{round}_w#{w}_#{i}".to_sym  # find existing -> deref sym_set
      end
      GC.start
      :ok
    end
  end

  builder.value  # builder dies -> its objspace (may own the live sym_set) orphaned
  20.times do
    100.times { |i| _ = "main_r#{round}_#{i}".to_sym }
    GC.start(full_mark: true, immediate_sweep: true)
    GC.compact rescue nil
  end
  others.each(&:value)
end
GC.start(full_mark: true)
GC.compact rescue nil
puts "done #{Symbol.all_symbols.size}"