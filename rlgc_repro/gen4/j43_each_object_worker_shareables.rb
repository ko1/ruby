# Main each_object sees shareable objects created by worker Ractors
# axes: each_object shareables, 4 ractors
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
class STag; end
port = Ractor::Port.new
ws = 4.times.map do |rid|
  Ractor.new(port, rid) do |p, id|
    made = 20.times.map { Ractor.make_shareable(STag.new) }
    p.send(made)  # keep alive on main via port
    made.size
  end
end
kept = 4.times.map { port.receive }
raise unless ws.map(&:value).all? { |v| v == 20 }
GC.start
total = kept.map(&:size).sum
c = ObjectSpace.each_object(STag) { }
raise "count #{c} < #{total}" unless c >= total
raise unless total == 4 * 20
puts "OK j43_each_object_worker_shareables"
