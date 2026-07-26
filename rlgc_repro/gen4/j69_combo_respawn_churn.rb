# High-churn combined respawn across many short Ractors
# axes: finalizer+weakmap, respawn churn
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
exp = (0...70).count { |i| i % 3 == 0 }
5.times do |w|
  port = Ractor::Port.new
  rs = 2.times.map do |k|
    Ractor.new(port, w, k) do |p, wv, kk|
      wm = ObjectSpace::WeakMap.new
      keep = []
      70.times do |i|
        o = Object.new
        ObjectSpace.define_finalizer(o, proc { })
        wm[i] = o
        keep << o if i % 3 == 0
      end
      GC.start
      ok = (0...70).select { |i| i % 3 == 0 }.all? { |i| wm.key?(i) }
      p.send(ok)
      keep.size
    end
  end
  2.times { raise unless port.receive == true }
  raise unless rs.map(&:value).all? { |v| v == exp }
  GC.start
end
puts "OK j69_combo_respawn_churn"
