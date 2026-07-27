# c52: scatter over a frozen shareable table: workers receive only index ranges
# and read BIGTBL in place (no chunk copying); partials gathered and checked.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

W = STRESS ? 3 : 4
PER = STRESS ? 12 : 80
BIGTBL = Ractor.make_shareable(Array.new(W * PER) { |i| [i, (i * 11) % 13, "e#{i}"] })

gather = Ractor::Port.new
ws = W.times.map do |i|
  Ractor.new(gather, i, i * PER, PER) do |g, wid, lo, n|
    acc = 0
    n.times do |k|
      row = BIGTBL[lo + k]
      raise "row id" unless row[0] == lo + k
      raise "row name" unless row[2] == "e#{lo + k}"
      acc += row[1]
    end
    g << [:partial, wid, acc]
    :tbl_done
  end
end

total = 0
W.times do
  tag, _wid, acc = gather.receive
  raise "partial" unless tag == :partial
  total += acc
end
raise "total" unless total == BIGTBL.sum { |r| r[1] }
GC.stress = false
ws.each { |r| raise unless r.value == :tbl_done }
GC.compact unless STRESS
puts "OK c52_scatter_shared_table"
