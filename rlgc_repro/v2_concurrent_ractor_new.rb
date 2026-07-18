Warning[:experimental] = false
# 同一 Ractor(main) の複数 thread が同時に Ractor.new → creating_child_objspace 上書き衝突
gcth = Thread.new { 200.times { GC.start; GC.compact rescue nil; Thread.pass } }
threads = 4.times.map do
  Thread.new do
    30.times do
      r = Ractor.new { Array.new(200){ +"x#{_1}" }; :ok }
      r.value
    end
  end
end
threads.each(&:join)
gcth.kill; gcth.join
puts "CONCURRENT_NEW_OK"
