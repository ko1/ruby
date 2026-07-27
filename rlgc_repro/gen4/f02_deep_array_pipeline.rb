# f02 transformer pipeline: depth-7 nested Arrays moved through 2-stage chain
# axes: move, chain lifecycle, GC.start between rounds
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

def nest(depth)
  return [depth, "leaf#{depth}"] if depth == 0
  [depth, nest(depth - 1), nest(depth - 1)]
end

def depth_of(aa)
  return 1 unless aa[1].is_a?(Array)
  1 + depth_of(aa[1])
end

out = Ractor::Port.new
stage2 = Ractor.new(out) do |po|
  loop do
    mm = Ractor.receive
    break if mm == :eof
    mm[0] += 1000 # mutate moved object in place
    po.send(mm, move: true)
  end
end
stage1 = Ractor.new(stage2) do |nxt|
  loop do
    mm = Ractor.receive
    if mm == :eof
      nxt.send(:eof)
      break
    end
    mm << :stamped
    nxt.send(mm, move: true)
  end
end

rounds = STRESS ? 2 : 4
ndepth = STRESS ? 5 : 7
rounds.times do |i|
  gr = nest(ndepth)
  stage1.send(gr, move: true)
  begin
    gr.size
    raise "source not husked"
  rescue Ractor::MovedError
  end
  res = out.receive
  assert res[0] == 1000 + ndepth, "stage2 mutation"
  assert res.last == :stamped, "stage1 mutation"
  assert depth_of(res) == ndepth + 1, "depth preserved"
  GC.start if i.odd?
end
stage1.send(:eof)
puts "OK f02_deep_array_pipeline"
