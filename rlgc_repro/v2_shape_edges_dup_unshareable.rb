# Crashed with "[BUG] try to mark T_NONE object" in the global GC's
# shape_tree_mark_and_move: rb_managed_id_table_dup (the COW growth of
# a shape's edge table, taken whenever a Ractor inserts a transition
# into a shape that already has a multi-edge table) forgot the
# born-shareable pin that rb_managed_id_table_create has, so the new
# generation -- allocated in the inserting worker's objspace and
# reachable only from the VM-global shape tree -- was freed by the
# worker's local GC while installed (design_v2.md section 2.4-1).
#
# Multi-Ractor generic-ivar churn drives exactly those shape
# transitions (String/Array hosts: set, read, drop hosts so local
# sweeps delete) while main reads ivars of frozen shareable hosts and
# everyone GCs; the worker-driven full GC then marks the stale edge
# table. Also exercises the generic_fields_lock added for M1b.
sh = 100.times.map do |i|
  s = "host-#{i}"
  s.instance_variable_set(:@tag, "tag-#{i}".freeze)
  Ractor.make_shareable(s)
end

rs = 8.times.map do
  Ractor.new do
    ring = []
    30_000.times do |k|
      h = "h#{k}"             # String host
      h.instance_variable_set(:@a, [k, k * 2])
      h.instance_variable_set(:@b, "v#{k}")
      a = [k]                  # Array host
      a.instance_variable_set(:@c, k)
      ring << h << a
      ring.shift(2) if ring.size > 64
      if k % 7_000 == 0
        GC.start(full_mark: false)
        GC.start if k % 21_000 == 0
      end
    end
    :done
  end
end

ok = true
60.times do
  sh.each_with_index { |s, i| ok &&= (s.instance_variable_get(:@tag) == "tag-#{i}") }
  10_000.times { |k| t = +"m#{k}"; t.instance_variable_set(:@m, k) }
  GC.start(full_mark: false)
end
rs.each(&:join)
raise "shareable ivar mismatch" unless ok
puts "M1B_GEN_OK"
