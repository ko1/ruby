# Proven s7-family reproducer (the most reliable trigger; all structural amplifications I tried
# suppressed it). RUN SOLO (concurrency suppresses the bug) with the doubled-rate recipe:
#   RUBY_RACTOR_LOCAL_GC=1 RUBY_GC_HEAP_INIT_SLOTS=2000 /home/ko1/ruby/src/master/ruby THIS.rb
# Repeat >=20x; ~14% hit a `[BUG] try to mark T_NONE object (obj: out-of-heap, parent: out-of-heap)`
# (a 40-byte page-tail shareable leaf freed while a foreign parent keeps the edge).

NPROD     = 18      # >= cores (16)
NHOLD     = 6
ROUNDS    = 28
DEPTH     = 4
NIV       = 9       # generic ivars per node -> generic_fields_tbl / imemo_fields

SE = Struct.new(:a, :b, :c)                              # embedded (<= 3 fields) -> generic_fields_tbl
SH = Struct.new(:f0, :f1, :f2, :f3, :f4, :f5, :f6, :f7) # heap-allocated ptr (> 3 fields) -> as.heap.fields_obj
DD = Data.define(:l, :r, :tag)                           # Data -> RTYPEDDATA.fields_obj

IVN = Ractor.make_shareable((0...NIV).map { |i| :"@iv#{i}" })

# attach NIV generic ivars (each a frozen ref payload the GC must mark) -> the lost LEAF nodes
def deck(obj, seed, ivn)
  ivn.each_with_index do |nm, i|
    obj.instance_variable_set(nm, [seed, i, "p#{i}".freeze].freeze)
  end
  obj
end

# deep nested DAG mixing embedded Struct, heap Struct, Data, and generic ivars
def build(depth, seed, ivn)
  if depth <= 0
    e = deck(SE.new("leaf".freeze, depth, :leaf), seed, ivn)
    return DD.new(e, deck(SH.new(0, 1, 2, 3, 4, 5, 6, e), seed, ivn), :leaf)
  end
  l = build(depth - 1, seed, ivn)
  r = build(depth - 1, seed, ivn)
  e  = deck(SE.new(l, r, depth), seed, ivn)
  h  = deck(SH.new(l, r, depth, :node, e, l, r, e), seed, ivn)
  e2 = deck(SE.new(h, l, r), seed, ivn)
  DD.new(e2, h, depth)
end

# HOLDER: roots received shareable graphs in a bounded window, reads embedded/heap/Data
# and generic-ivar fields cross-objspace, churns (drops oldest), and GCs hard (full_mark -> global).
holders = (0...NHOLD).map do |i|
  Ractor.new(name: "hold#{i}") do
    keep = []; cnt = 0
    loop do
      m = Ractor.receive
      break if m == :stop
      keep << m; cnt += 1
      if m.respond_to?(:l)
        _ = m.l.a                                   # embedded field
        _ = m.r.f7 rescue nil                       # heap field
        _ = m.l.instance_variable_get(:@iv3) rescue nil  # generic-ivar LEAF
      end
      keep.shift while keep.size > 16               # churn -> reclaim dead subtrees
      GC.start(full_mark: cnt % 6 == 0) if cnt % 3 == 0
    end
    keep.size
  end
end

# relentless GC pressure from the main Ractor (minor + global)
main_hammer = Thread.new { 900.times { GC.start(full_mark: false) } }
g_hammer    = Thread.new { 240.times { GC.start(full_mark: true) } }

# PRODUCER: each round build+share a graph two ways:
#  (a) inline in this producer's objspace, then fan out and drop;
#  (b) in a short-lived sub-ractor that runs a LOCAL minor GC (promotes subtree to OLD) and
#      then DIES -> ORPHANED objspace still owning the live shareable subtree (load-bearing).
producers = (0...NPROD).map do |w|
  Ractor.new(w, holders, IVN, ROUNDS, DEPTH) do |wid, hs, ivn, rounds, depth|
    pending = []
    rounds.times do |r|
      g = Ractor.make_shareable(build(depth, wid * 10000 + r, ivn))
      hs.each_with_index { |h, hi| h.send(g) if (wid + hi + r) % 2 == 0 }
      g = nil

      pending << Ractor.new(hs, ivn, depth, wid * 10000 + r) do |hs2, ivn2, d, seed|
        root = Ractor.make_shareable(build(d, seed, ivn2))
        hs2.each_with_index { |h, hi| h.send(root) if (seed + hi) % 2 == 0 }
        root = nil
        GC.start(full_mark: false)                 # promote to OLD, then objspace orphaned here
        :died
      end
      if pending.size >= 6
        pending.shift.value
      end

      GC.start(full_mark: false) if r % 2 == 0
      GC.start(full_mark: true)  if r % 9 == 0
    end
    pending.each(&:value)
    :done
  end
end

producers.each(&:value)
main_hammer.join
g_hammer.join
holders.each { |h| h.send(:stop) }
holders.each(&:value)
GC.start(full_mark: true)
puts "ok"