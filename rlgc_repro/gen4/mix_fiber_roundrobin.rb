# gen4 mixed-runtime: scheduler-ish loop — each worker ractor multiplexes 8
# cooperative fibers round-robin, each fiber owning per-task state; fibers
# yield between steps like an event loop.
# axes: transfer=copy(results), GC=GC.compact once mid-loop per worker, runtime=many fibers
N_WORKERS = 3
N_FIBERS = 8
STEPS = 50

workers = N_WORKERS.times.map do |wid|
  Ractor.new(wid, N_FIBERS, STEPS) do |id, nf, steps|
    fibers = nf.times.map do |f|
      Fiber.new do
        state = { fid: f, acc: 0, log: [] }
        steps.times do |s|
          state[:acc] += id * 100 + f + s
          state[:log] << "s#{s}"
          state[:log].shift if state[:log].size > 5
          Fiber.yield
        end
        state[:acc]
      end
    end
    done = Array.new(nf)
    rounds = 0
    until fibers.compact.empty?
      fibers.each_with_index do |fb, f|
        next unless fb
        v = fb.resume
        unless fb.alive?
          done[f] = v
          fibers[f] = nil
        end
      end
      rounds += 1
      GC.compact if rounds == steps / 2
    end
    done.sum
  end
end

got = workers.sum(&:value)
exp = 0
N_WORKERS.times do |id|
  N_FIBERS.times do |f|
    STEPS.times { |s| exp += id * 100 + f + s }
  end
end
raise "FAIL #{got} != #{exp}" unless got == exp
puts "OK mix_fiber_roundrobin"
