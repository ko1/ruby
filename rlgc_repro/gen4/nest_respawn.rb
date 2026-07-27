# gen4 nested: mid-level supervisors respawn their failing leaves internally
# (first attempt of odd leaves dies); main only sees clean totals, plus a
# death-count audit per supervisor.
# axes: transfer=copy, GC=GC.start after leaf respawn, exceptions=leaf death+respawn, depth=2
N_SUP = 3
N_LEAF = 4
UNITS = 60

sups = N_SUP.times.map do |sid|
  Ractor.new(sid, N_LEAF, UNITS) do |sup_id, nleaf, units|
    mk_leaf = lambda do |lid, att|
      Ractor.new(sup_id, lid, att, units) do |s, l, a, u|
        Thread.current.report_on_exception = false
        raise "leaf #{s}-#{l} croak" if a == 0 && l.odd?
        (0...u).sum { |i| (s + l) * 100 + i }
      end
    end
    live = {}
    nleaf.times { |l| live[mk_leaf.call(l, 0)] = [l, 0] }
    deaths = 0
    total = 0
    until live.empty?
      begin
        r, v = Ractor.select(*live.keys)
        live.delete(r)
        total += v
      rescue Ractor::RemoteError => e
        lid, att = live.delete(e.ractor)
        deaths += 1
        GC.start
        live[mk_leaf.call(lid, att + 1)] = [lid, att + 1]
      end
    end
    [sup_id, total, deaths]
  end
end

exp_deaths_per_sup = (0...N_LEAF).count(&:odd?)
grand = 0
sups.each do |s|
  sid, total, deaths = s.value
  exp = (0...N_LEAF).sum { |l| (0...UNITS).sum { |i| (sid + l) * 100 + i } }
  raise "FAIL sup#{sid} total" unless total == exp
  raise "FAIL sup#{sid} deaths #{deaths}" unless deaths == exp_deaths_per_sup
  grand += total
end
raise "FAIL grand" unless grand > 0
puts "OK nest_respawn"
