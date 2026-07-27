# KV store holding Struct values; struct copied over both directions.
# Axes: shards=2, 80 puts/gets, Struct payload, stress in services, GC.compact client.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
Item = Struct.new(:id, :qty, :tags)
done = Ractor::Port.new
shards = 2.times.map do
  Ractor.new(done, STRESS) do |done, stress|
    GC.stress = true if stress
    db = {}
    loop do
      msg = Ractor.receive
      break if msg == :stop
      op, k, v, rp = msg
      case op
      when :put then db[k] = v; rp << v.qty
      when :get then rp << db[k]
      end
    end
    GC.stress = false
    done << :done
    db.values.sum(&:qty)
  end
end
rp = Ractor::Port.new
80.times do |i|
  it = Item.new(i, i * 2, ["a#{i}", "b#{i}"])
  shards[i % 2].send([:put, "k#{i}", it, rp])
  raise "put#{i}" unless rp.receive == i * 2
  GC.compact if i == 40
end
80.times do |i|
  shards[i % 2].send([:get, "k#{i}", nil, rp])
  it = rp.receive
  raise "get#{i}" unless it.is_a?(Item) && it.id == i && it.qty == i * 2 && it.tags == ["a#{i}", "b#{i}"]
end
shards.each { _1.send(:stop) }
2.times { done.receive }
raise unless shards.map(&:value).sum == 80.times.sum { _1 * 2 }
puts "OK d09_kv_struct_values"
