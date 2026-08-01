# f25 metadata ETL: mixed genivar carriers (String/Array/Hash) through 2-stage pipeline + GC.compact
# axes: copy then move, generic ivars, chain, GC.compact per stage
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

out = Ractor::Port.new
s2 = Ractor.new(out) do |po|
  loop do
    mm = Ractor.receive
    break if mm == :eof
    GC.compact
    po.send(mm.map { |oo| [oo.class.name, oo.instance_variable_get(:@meta)] })
  end
end
s1 = Ractor.new(s2) do |nxt|
  loop do
    mm = Ractor.receive
    if mm == :eof
      nxt.send(:eof)
      break
    end
    GC.compact
    nxt.send(mm, move: true) # stage1 got a copy; moves it onward
  end
end

carriers = [+"str-carrier", [1, 2, 3], { k: :v }]
carriers.each_with_index { |cc, i| cc.instance_variable_set(:@meta, "m#{i}") }
s1.send(carriers)
back = out.receive
assert back == [["String", "m0"], ["Array", "m1"], ["Hash", "m2"]], "pipeline meta #{back.inspect}"
# originals untouched (stage1 moved its own copy)
carriers.each_with_index { |cc, i| assert cc.instance_variable_get(:@meta) == "m#{i}", "source #{i} intact" }
s1.send(:eof)
puts "OK f25_genivar_pipeline_compact"
