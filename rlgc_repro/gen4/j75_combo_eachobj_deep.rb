# Few workers export many shareables while registering finalizers
# axes: finalizer + each_object shareables, 2 ractors, deep
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
class ETag; end
port = Ractor::Port.new
ws = 2.times.map do |rid|
  Ractor.new(port, rid) do |p, id|
    made = 50.times.map do
      o = Ractor.make_shareable(ETag.new)
      o
    end
    # finalizers on own (unshared) objects
    50.times { x = Object.new; ObjectSpace.define_finalizer(x, proc { }) }
    GC.start
    p.send(made)
    made.size
  end
end
kept = 2.times.map { port.receive }
raise unless ws.map(&:value).all? { |v| v == 50 }
GC.compact
total = kept.map(&:size).sum
c = ObjectSpace.each_object(ETag) { }
raise "count #{c} < #{total}" unless c >= total
raise unless total == 2 * 50
puts "OK j75_combo_eachobj_deep"
