# Many workers produce shareables; main counts them via each_object
# axes: each_object shareables, 8 ractors
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
class STag; end
port = Ractor::Port.new
ws = 8.times.map do |rid|
  Ractor.new(port, rid) do |p, id|
    made = 12.times.map { Ractor.make_shareable(STag.new) }
    p.send(made)  # keep alive on main via port
    made.size
  end
end
kept = 8.times.map { port.receive }
raise unless ws.map(&:value).all? { |v| v == 12 }
GC.start
total = kept.map(&:size).sum
c = ObjectSpace.each_object(STag) { }
raise "count #{c} < #{total}" unless c >= total
raise unless total == 8 * 12
puts "OK j44_each_object_shareables_wide"
