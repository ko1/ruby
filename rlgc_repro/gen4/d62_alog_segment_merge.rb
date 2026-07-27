# Segmented append-only log: seal a segment every 25 records; :merge folds all
# sealed segments into one compacted segment (latest per key). Axes: 120 appends,
# copy, stress in service, GC.compact after merge.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
done = Ractor::Port.new
svc = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  sealed = []
  active = []
  loop do
    msg = Ractor.receive
    break if msg == :stop
    op, k, v, rp = msg
    case op
    when :append
      active << [k, v]
      if active.size == 25
        sealed << active
        active = []
      end
      rp << [sealed.size, active.size]
    when :merge
      merged = {}
      sealed.each { |seg| seg.each { |mk, mv| merged[mk] = mv } }
      sealed = [merged.to_a]
      GC.compact
      rp << merged.size
    when :lookup
      src = sealed.flatten(1) + active
      ent = src.reverse_each.find { |lk, _| lk == k }
      rp << (ent && ent[1])
    end
  end
  GC.stress = false
  done << :done
  [sealed.size, active.size]
end
rp = Ractor::Port.new
rng = Random.new(62)
model = {}
sealed_model = {}
120.times do |i|
  k = "k#{rng.rand(9)}"
  v = i
  model[k] = v
  svc.send([:append, k, v, rp])
  nseal, nact = rp.receive
  raise "seg#{i}" unless nseal == (i + 1) / 25 && nact == (i + 1) % 25
  sealed_model[k] = v if i < 100
end
svc.send([:merge, nil, nil, rp])
raise "merged size" unless rp.receive == sealed_model.size
model.each_key do |k|
  svc.send([:lookup, k, nil, rp])
  raise "lookup #{k}" unless rp.receive == model[k]
end
svc.send(:stop)
done.receive
raise unless svc.value == [1, 20]
puts "OK d62_alog_segment_merge"
