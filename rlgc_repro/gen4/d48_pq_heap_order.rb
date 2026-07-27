# Priority queue service (binary heap); seeded pushes then pop-all must come out
# sorted ascending. Axes: 120 items, copy, stress in service.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
class Heap
  def initialize = @a = []
  def size = @a.size
  def push(x)
    @a << x
    i = @a.size - 1
    while i > 0 && @a[(i - 1) / 2] > @a[i]
      @a[(i - 1) / 2], @a[i] = @a[i], @a[(i - 1) / 2]
      i = (i - 1) / 2
    end
  end
  def pop
    return nil if @a.empty?
    top = @a[0]
    last = @a.pop
    unless @a.empty?
      @a[0] = last
      i = 0
      loop do
        c = 2 * i + 1
        break if c >= @a.size
        c += 1 if c + 1 < @a.size && @a[c + 1] < @a[c]
        break if @a[i] <= @a[c]
        @a[i], @a[c] = @a[c], @a[i]
        i = c
      end
    end
    top
  end
end
done = Ractor::Port.new
svc = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  h = Heap.new
  loop do
    msg = Ractor.receive
    break if msg == :stop
    op, x, rp = msg
    case op
    when :push then h.push(x); rp << h.size
    when :pop then rp << h.pop
    end
  end
  GC.stress = false
  done << :done
  h.size
end
rp = Ractor::Port.new
rng = Random.new(48)
vals = Array.new(120) { rng.rand(10_000) }
vals.each_with_index do |v, i|
  svc.send([:push, v, rp])
  raise unless rp.receive == i + 1
end
out = Array.new(120) { svc.send([:pop, nil, rp]); rp.receive }
raise "order" unless out == vals.sort
svc.send([:pop, nil, rp])
raise "not empty" unless rp.receive.nil?
svc.send(:stop)
done.receive
raise unless svc.value == 0
puts "OK d48_pq_heap_order"
