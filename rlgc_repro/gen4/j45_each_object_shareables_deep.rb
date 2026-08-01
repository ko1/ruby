# Few workers produce many shareables each; main lower-bound count
# axes: each_object shareables, 2 ractors, deep
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
class STag; end
port = Ractor::Port.new
ws = 2.times.map do |rid|
  Ractor.new(port, rid) do |p, id|
    made = 60.times.map { Ractor.make_shareable(STag.new) }
    p.send(made)  # keep alive on main via port
    made.size
  end
end
kept = 2.times.map { port.receive }
raise unless ws.map(&:value).all? { |v| v == 60 }
GC.start
total = kept.map(&:size).sum
c = ObjectSpace.each_object(STag) { }
raise "count #{c} < #{total}" unless c >= total
raise unless total == 2 * 60
puts "OK j45_each_object_shareables_deep"
