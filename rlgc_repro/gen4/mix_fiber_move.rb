# gen4 mixed-runtime: worker ractors assemble result buffers ACROSS fiber
# switches (buffer built piecewise by 4 fibers) and then MOVE the finished
# buffer out to the collector port.
# axes: transfer=move(out), GC=GC.start between assemblies, runtime=fibers building moved objects
N_WORKERS = 3
N_BUFS = 40

out = Ractor::Port.new
workers = N_WORKERS.times.map do |wid|
  Ractor.new(out, wid, N_BUFS) do |o, id, nbufs|
    nbufs.times do |b|
      buf = +"w#{id}b#{b}:"
      parts = 4.times.map do |f|
        Fiber.new do
          2.times do |k|
            buf << "f#{f}k#{k};"
            Fiber.yield
          end
        end
      end
      # interleave fiber steps so appends alternate across fibers
      3.times { parts.each { |fb| fb.resume if fb.alive? } }
      GC.start if b % 15 == 14
      o.send(buf, move: true)
    end
    :done
  end
end

per_buf_payload = 4 * 2 * "fXkY;".size
total = 0
(N_WORKERS * N_BUFS).times do
  buf = out.receive
  head, rest = buf.split(":", 2)
  raise "FAIL head #{head}" unless head.match?(/\Aw\db\d+\z/)
  raise "FAIL body len" unless rest.size == per_buf_payload
  raise "FAIL body" unless rest.scan(/f(\d)k(\d);/).size == 8
  total += 1
end
workers.each { |w| raise "FAIL" unless w.value == :done }
raise "FAIL total" unless total == N_WORKERS * N_BUFS
puts "OK mix_fiber_move"
