# gen4 mixed-runtime: each ractor runs 3 threads that allocate churn while a
# 4th thread calls GC.start on a cadence; one ractor also does GC.compact at
# the end with all thread stacks live.
# axes: transfer=copy(results), GC=GC.start from non-main thread + compact, runtime=threads
N_RACTORS = 3
ITER = 300

workers = N_RACTORS.times.map do |wid|
  Ractor.new(wid, ITER) do |id, iter|
    stop = false
    gc_thread = Thread.new do
      n = 0
      until stop
        GC.start
        n += 1
        Thread.pass
        sleep 0.001
      end
      n
    end
    churners = 3.times.map do |t|
      Thread.new do
        keep = []
        acc = 0
        iter.times do |i|
          keep << "chunk-#{id}-#{t}-#{i}" * 2
          keep.shift if keep.size > 25
          acc += keep.last.size
        end
        acc
      end
    end
    total = churners.sum(&:value)
    stop = true
    gcs = gc_thread.value
    GC.compact if id == 1
    [total, gcs]
  end
end

exp = Array.new(N_RACTORS) do |id|
  3.times.sum do |t|
    ITER.times.sum { |i| ("chunk-#{id}-#{t}-#{i}" * 2).size }
  end
end
workers.each_with_index do |w, id|
  total, gcs = w.value
  raise "FAIL total r#{id}" unless total == exp[id]
  raise "FAIL gc thread never ran" unless gcs >= 1
end
puts "OK mix_gc_thread_storm"
