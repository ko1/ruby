# Wide fan: finalizers plus exported shareables counted on main
# axes: finalizer + each_object shareables, 7 ractors
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
class ETag; end
port = Ractor::Port.new
ws = 7.times.map do |rid|
  Ractor.new(port, rid) do |p, id|
    made = 14.times.map do
      o = Ractor.make_shareable(ETag.new)
      o
    end
    # finalizers on own (unshared) objects
    14.times { x = Object.new; ObjectSpace.define_finalizer(x, proc { }) }
    GC.start
    p.send(made)
    made.size
  end
end
kept = 7.times.map { port.receive }
raise unless ws.map(&:value).all? { |v| v == 14 }
GC.compact
total = kept.map(&:size).sum
c = ObjectSpace.each_object(ETag) { }
raise "count #{c} < #{total}" unless c >= total
raise unless total == 7 * 14
puts "OK j74_combo_eachobj_wide"
