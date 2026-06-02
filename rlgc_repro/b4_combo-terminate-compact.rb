# Target: termination x compaction (Ractor-local GC).
# GC.auto_compact=true makes a GLOBAL full GC set during_compacting=TRUE on the MAIN
# objspace, bypassing the rlgc_has_local guard that gc_compact() uses to disable moving.
# The post-move fixup gc_update_references() runs ONLY on the main objspace's heaps, so
# references stored in ORPHANED (terminated-Ractor) objspaces that point at MOVED
# main-objspace objects are never updated -> dangling pointer -> SEGV on the next GC.
# Run: RUBY_RACTOR_LOCAL_GC=1 ruby this.rb   (also try RUBY_GC_HEAP_INIT_SLOTS=1000,
# RUBY_GC_STRESS=1). Crashes 5/5 default heap; control with auto_compact=false prints "ok".

GC.auto_compact = true

NWAVES = 40
RACTORS_PER_WAVE = 6

# Holder in MAIN keeps shareable graphs (which live in worker objspaces) reachable across
# waves, so terminated workers' objspaces stay orphaned with live cross-objspace edges.
$holder = []

def build_graph
  parts = []
  100.times do |i|
    s = "node-#{i}-#{'x' * (i % 40)}".freeze
    a = [s, i, :"sym#{i % 16}"].freeze
    parts << a
  end
  g = parts.freeze
  Ractor.make_shareable(g)   # shareable subtree, lives in this worker's objspace
  g
end

# Hammer: continuous global full GCs (every full GC is a GLOBAL STW GC here).
hammer = Thread.new do
  loop do
    GC.start(full_mark: true, immediate_sweep: true)
    Thread.pass
  end
end

NWAVES.times do |w|
  # Wave of workers: build shareable graphs, send them to main, then TERMINATE -> orphans.
  rs = RACTORS_PER_WAVE.times.map do
    Ractor.new do
      graphs = []
      30.times { graphs << build_graph }
      GC.start rescue nil
      Ractor.main.send(graphs.freeze, move: false) rescue nil
      graphs.size
    end
  end
  rs.each do
    begin
      $holder << Ractor.receive
    rescue
    end
  end
  rs.each { |r| r.take rescue nil }   # wave fully terminates -> objspaces orphaned

  # Compact MAIN (moves objects) while orphans hold cross-objspace shareable edges and
  # main objects referenced from orphans are being relocated; interleave global full GCs.
  6.times do
    GC.compact
    GC.start(full_mark: true)
  end

  # Touch holder contents to force marking through the cross-objspace edges.
  $holder.each do |arr|
    arr.each { |g| g.each { |n| n[0].length } } rescue nil
  end

  # Drop old graphs so orphan objspaces get a live/dead mix and pages may be reclaimed.
  $holder.shift if $holder.size > 12
end

hammer.kill rescue nil
GC.compact
GC.start(full_mark: true)
puts "ok"
