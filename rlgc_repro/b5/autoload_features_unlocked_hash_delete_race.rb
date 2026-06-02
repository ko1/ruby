# b6 scenario 8 (DISTINCT Face B on autoload_features): rb_const_remove of an AUTOLOAD constant
# runs UNLOCKED (no autoload_mutex, no VM lock at the callsite, variable.c:3649):
#   -> rb_clear_constant_cache_for_id(id)            (unlocked set_table_foreach walk)
#   -> autoload_delete(mod,id) -> rb_hash_delete(autoload_features)   (unlocked mutation of the
#      VM-GLOBAL ident hash `autoload_features`)
# We have MANY non-main Ractors concurrently call remove_const on autoload constants of a SHAREABLE
# module (allowed; each does the two unlocked VM-global mutations above) while MAIN concurrently
# RE-ARMS via Module#autoload -> autoload_feature_lookup_or_create -> rb_hash_aset(autoload_features)
# (under autoload_mutex, but NOT under whatever protects the concurrent unlocked rb_hash_delete).
# Concurrent unlocked aset/delete on the same VM-global hash => backing realloc/rehash corruption =>
# SEGV/Aborted, OR the const-cache set_table walk corruption. Only MAIN writes via Module#autoload
# (single autoload writer) => no global-autoload_mutex deadlock among writers. Hammer GC + compact.

M = Module.new
Ractor.make_shareable(M)
Object.const_set(:Shared8, M)
NKEY = 40

# pre-arm all keys so workers have autoload consts to remove
NKEY.times { |k| M.autoload(:"A#{k}", "/no/such/a#{k}") rescue nil }

stop = false
gc = Thread.new do
  until stop
    GC.start(full_mark: true, immediate_sweep: true)
    GC.compact rescue nil
  end
end

# worker Ractors: hammer remove_const of autoload consts on the shareable module (unlocked
# rb_hash_delete(autoload_features) + unlocked const-cache clear), concurrent across 18 Ractors.
removers = 18.times.map do
  Ractor.new(M, NKEY) do |mod, nk|
    6000.times do
      k = rand(nk)
      begin
        mod.send(:remove_const, :"A#{k}")
      rescue
      end
    end
    :ok
  end
end

# MAIN: sole Module#autoload re-armer (re-inserts into autoload_features under autoload_mutex),
# racing the workers' unlocked rb_hash_delete of the same VM-global hash.
arm = Thread.new do
  20000.times do |i|
    k = i % NKEY
    begin
      M.autoload(:"A#{k}", "/no/such/a#{k}_#{i}") unless M.autoload?(:"A#{k}")
    rescue
    end
  end
end

removers.each { |r| r.value rescue nil }
arm.join
stop = true
gc.join
puts "OK"
