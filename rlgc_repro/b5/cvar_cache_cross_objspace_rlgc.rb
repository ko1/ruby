# cv9 — combined maximal-stress cvar-cache scenario (NO crash found).
# Probes: cross-objspace ent->cref dangling, cvc-table dup/swap race,
# in-place cvar mutation vs concurrent non-main read, include/prepend cvar-cache
# invalidation, global_cvar_state churn, GC.compact + full GC concurrent.
NMOD = 16
SECS = 6.0

$mods = (0...NMOD).map do |i|
  m = Module.new
  m.class_variable_set(:@@cv, ("sv_%d" % i).freeze)
  Ractor.make_shareable(m)
  m
end
$hosts = (0...NMOD).map do |i|
  c = Class.new
  c.class_variable_set(:@@h, i)
  c.class_eval("def rdh; @@h; end; def self.srh; @@h; end")
  Object.const_set("Hh#{i}", c)
  c
end
$inc = (0...NMOD).map { Module.new { class_variable_set(:@@m, 0) } }

stop = false
hammer = Thread.new { until stop; GC.start(full_mark: true, immediate_sweep: true); end }
compactor = Thread.new { until stop; GC.compact rescue nil; end }

# non-main readers: short-lived, cross-objspace cref, hammering shareable cvar
spawner = Thread.new do
  until stop
    rs = (0...NMOD).map do |i|
      m = $mods[i]
      Ractor.new(m, i) do |mod, idx|
        rd = mod.module_eval("proc { @@cv }", "r#{idx}", 1)  # cref in THIS objspace
        25.times { rd.call }
        :ok
      end
    end
    rs.each { |r| r.value rescue nil }   # die -> orphan objspaces
  end
end

# main writer: cvar churn (dup/swap, reshape, state++) + include/prepend churn
writer = Thread.new do
  k = 0
  until stop
    NMOD.times do |i|
      $hosts[i].class_variable_set(:"@@w#{k}", k)
      $hosts[i].srh; $hosts[i].new.rdh
      begin
        if k.even? then $hosts[i].include($inc[i]) else $hosts[i].prepend($inc[i]) end
      rescue
      end
    end
    k += 1
    if k % 30 == 0
      $hosts.each do |c|
        c.class_variables.each do |n|
          next if n == :@@h
          c.remove_class_variable(n) rescue nil
        end
      end
    end
  end
end

t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
until Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0 > SECS
  Thread.pass
end
stop = true
spawner.join; writer.join; hammer.join; compactor.join
puts "OK"
