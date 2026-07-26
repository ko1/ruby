# Data.define の単方向リストを make_shareable、reader が全走査
# axes: len=50 readers=8 compacts=8 data linked
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
LNode = Data.define(:v, :nxt)
LEN = 50
head = nil
(LEN - 1).downto(0) { |i| head = LNode.new(v: i, nxt: head) }
LIST = Ractor.make_shareable(head)
EXP = (0...LEN).sum
rs = 8.times.map do |rid|
  Ractor.new(LIST, rid) do |h, id|
    acc = 0
    cur = h
    while cur
      acc += cur.v
      cur = cur.nxt
    end
    acc
  end
end
8.times { GC.compact }
rs.each { |ra| raise "mismatch" unless ra.value == EXP }

puts "OK i57_linked_list_data"
