# KV service validating puts against a frozen shareable schema (field/type check).
# Axes: 1 service, 80 puts (20 invalid), make_shareable schema, stress in service.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
SCHEMA = Ractor.make_shareable({ name: String, qty: Integer, tags: Array })
done = Ractor::Port.new
svc = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  db = {}
  rejected = 0
  loop do
    msg = Ractor.receive
    break if msg == :stop
    op, k, v, rp = msg
    case op
    when :put
      if SCHEMA.all? { |f, t| v.key?(f) && v[f].is_a?(t) }
        db[k] = v; rp << :ok
      else
        rejected += 1; rp << :bad
      end
    when :get then rp << db[k]
    end
  end
  GC.stress = false
  done << :done
  [db.size, rejected]
end
rp = Ractor::Port.new
80.times do |i|
  v = if i % 4 == 3
        { name: "n#{i}", qty: "not-int", tags: [] }
      else
        { name: "n#{i}", qty: i, tags: ["t#{i}"] }
      end
  svc.send([:put, "k#{i}", v, rp])
  want = i % 4 == 3 ? :bad : :ok
  raise "resp#{i}" unless rp.receive == want
end
svc.send([:get, "k0", nil, rp])
raise unless rp.receive[:qty] == 0
svc.send(:stop)
done.receive
raise unless svc.value == [60, 20]
puts "OK d06_kv_frozen_schema"
