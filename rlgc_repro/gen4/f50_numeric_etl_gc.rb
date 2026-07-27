# f50 numeric ETL: extract(ints) -> transform(rationals) -> load(floats) with GC.start/GC.compact scattered
# axes: copy, mixed numerics through 2-stage chain, heavy GC punctuation
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

out = Ractor::Port.new
loader = Ractor.new(out) do |po|
  loop do
    mm = Ractor.receive
    break if mm == :eof
    GC.start
    po.send(mm.map { |rr| rr.to_f.round(4) })
  end
end
xform = Ractor.new(loader) do |nxt|
  loop do
    mm = Ractor.receive
    if mm == :eof
      nxt.send(:eof)
      break
    end
    GC.compact
    nxt.send(mm.map { |ii| Rational(ii, 16) })
  end
end

batches = STRESS ? 2 : 4
batches.times do |b|
  ints = (1..6).map { |i| i * (b + 1) }
  xform.send(ints)
  GC.start
  floats = out.receive
  want = ints.map { |ii| (Rational(ii, 16)).to_f.round(4) }
  assert floats == want, "batch #{b}: #{floats.inspect}"
  GC.compact if b.odd?
end
xform.send(:eof)
puts "OK f50_numeric_etl_gc"
