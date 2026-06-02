module AL; end
M = Module.new
Ractor.make_shareable(M)
Object.const_set(:S, M)
NK = 40
# pre-arm autoload constants so workers have entries to remove
NK.times { |k| M.autoload(:"A#{k}", "/no/such/a#{k}") rescue nil }

# 16 worker Ractors hammer remove_const of autoload consts on the SHAREABLE module.
# Each remove_const -> rb_const_remove (variable.c:3649, no lock) -> autoload_delete
#   -> rb_hash_delete(autoload_features)   [UNLOCKED mutation of the VM-global ident hash]
removers = 16.times.map do
  Ractor.new(M, NK) do |mod, nk|
    8000.times { begin; mod.send(:remove_const, :"A#{rand(nk)}"); rescue; end }
    :ok
  end
end

# MAIN is the SOLE Module#autoload re-armer (avoids the global autoload_mutex deadlock among
# multiple writers). Module#autoload -> autoload_feature_lookup_or_create
#   -> rb_hash_aset(autoload_features)     [holds autoload_mutex ONLY]
# Concurrent unlocked rb_hash_delete (workers) vs rb_hash_aset (main) realloc/free the SAME
# shared hash backing => glibc heap corruption ("double free" / "unaligned fastbin chunk").
arm = Thread.new do
  30000.times do |i|
    k = i % NK
    begin
      M.autoload(:"A#{k}", "/no/such/a#{k}_#{i}") unless M.autoload?(:"A#{k}")
    rescue
    end
  end
end

removers.each { |r| r.value rescue nil }
arm.join
puts "OK"
# Run: RUBY_RACTOR_LOCAL_GC=1 ruby this.rb   (17/17 completed runs crash; GC-independent.
# Also crashes under RUBY_GC_STRESS=1 RUBY_GC_HEAP_INIT_SLOTS=2000.)