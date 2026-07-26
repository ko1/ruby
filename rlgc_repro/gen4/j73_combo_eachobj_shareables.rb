# Workers register finalizers and export shareables; main counts via each_object
# axes: finalizer + each_object shareables, 4 ractors
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
class ETag; end
port = Ractor::Port.new
ws = 4.times.map do |rid|
  Ractor.new(port, rid) do |p, id|
    made = 20.times.map do
      o = Ractor.make_shareable(ETag.new)
      o
    end
    # finalizers on own (unshared) objects
    20.times { x = Object.new; ObjectSpace.define_finalizer(x, proc { }) }
    GC.start
    p.send(made)
    made.size
  end
end
kept = 4.times.map { port.receive }
raise unless ws.map(&:value).all? { |v| v == 20 }
GC.compact
total = kept.map(&:size).sum
c = ObjectSpace.each_object(ETag) { }
raise "count #{c} < #{total}" unless c >= total
raise unless total == 4 * 20
puts "OK j73_combo_eachobj_shareables"
