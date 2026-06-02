# RLGC crash: Ractor#send(move:true) transfers embedded->heap payload buffers
# (Array as.heap.ptr / String as.heap.ptr / Object as.heap.fields) across objspaces;
# concurrent local GC marks the out-of-heap moved graph and walks into a T_NONE child.
# Crashes: default 5/15, RUBY_GC_STRESS=1 3/5, RUBY_GC_HEAP_INIT_SLOTS=1000 4/5.
# Signature: [BUG] try to mark T_NONE object (parent: out-of-heap / T_ARRAY out-of-heap).

ROUNDS = 200
GRAPH_PER_ROUND = 8

# Build a graph of DISTINCT embed->heap objects (no aliasing across the top array).
def build_graph(seed)
  root = []
  6.times do |i|
    a = []
    24.times { |j| a << (+"x#{seed}_#{i}_#{j}" << ("q" * 24)) }   # embed -> heap array of heap strings
    8.times { a.push(a.length) }                                  # extra reallocs of the array body

    s = +"s"
    10.times { s << ("y" * 16) }                                  # embed -> heap string, repeated realloc

    o = Object.new
    50.times { |k| o.instance_variable_set("@v#{k}", +"iv#{k}" << "_") }  # embed -> heap.fields
    o.instance_variable_set(:@a, a)   # a/s reachable ONLY via o (no top-level aliasing)
    o.instance_variable_set(:@s, s)

    root << o
  end
  root
end

sink = Ractor.new do
  acc = 0
  while (g = Ractor.receive)
    break if g == :done
    g.each do |o|
      a = o.instance_variable_get(:@a)
      s = o.instance_variable_get(:@s)
      acc += a.length + s.bytesize
      acc += o.instance_variable_get(:@v49).bytesize
    end
    g = nil
  end
  acc
end

# Hammer thread: frequent explicit GC concurrent with the moves.
hammer = Thread.new do
  1000.times { GC.start; Thread.pass }
end

r = 0
while r < ROUNDS
  i = 0
  while i < GRAPH_PER_ROUND
    g = build_graph(r * 1000 + i)
    sink.send(g, move: true)   # transfers embed->heap payload buffers into sink objspace
    g = nil
    i += 1
  end
  r += 1
end

sink.send(:done)
sink.value
hammer.join
puts "ok"