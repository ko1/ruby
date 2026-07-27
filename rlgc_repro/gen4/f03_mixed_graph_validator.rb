# f03 validator pool: depth-6 mixed Hash/Array graph, 3 validators, copy fan-out
# axes: copy, pool lifecycle, deep equality, scattered GC.start
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

def mixed(depth)
  return { id: depth, vals: [1.5, :sym, "s#{depth}"] } if depth == 0
  { level: depth, items: [mixed(depth - 1), mixed(depth - 1)], meta: { d: depth } }
end

def count_leaves(gg)
  return 1 unless gg.key?(:items)
  gg[:items].sum { |cc| count_leaves(cc) }
end

DEPTH = STRESS ? 3 : 6
doc = mixed(DEPTH)
port = Ractor::Port.new
pool = 3.times.map do |wi|
  Ractor.new(port, wi, DEPTH) do |po, myid, dep|
    loop do
      mm = Ractor.receive
      break if mm == :eof
      ok = mm[:level] == dep && mm[:items].size == 2 && mm[:meta] == { d: dep }
      po.send([myid, ok, mm])
    end
  end
end

pool.each { |w| w.send(doc) }
GC.start
seen = {}
3.times do
  wid, ok, back = port.receive
  assert ok, "worker #{wid} validation"
  assert back == doc, "worker #{wid} deep equality"
  seen[wid] = true
end
assert seen.keys.sort == [0, 1, 2], "all workers reported"
assert count_leaves(doc) == 2**DEPTH, "leaf count"
pool.each { |w| w.send(:eof) }
puts "OK f03_mixed_graph_validator"
