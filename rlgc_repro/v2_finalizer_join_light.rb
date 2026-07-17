# p_finalizer_teardown の軽量版（速い反復用）。
# finalizer 付き Object を持つ短命 Ractor を終了→ value(join)→ GC を回し、
# 終了済み Ractor の main thread wrapper / default_port の UAF を突く。
Warning[:experimental] = false
20.times do
  rs = 4.times.map do |i|
    Ractor.new(i) do |x|
      arr = 12.times.map { o = Object.new; ObjectSpace.define_finalizer(o, proc { }); o }
      GC.start
      # 戻り値もヒープ複合オブジェクトにして join 経路を突く
      [("r%03d" % x) * 3, arr.size]
    end
  end
  rs.each { |r| v = r.value; raise "bad" unless v.is_a?(Array) && v[1] == 12 }
  GC.start
end
puts 'ok'
