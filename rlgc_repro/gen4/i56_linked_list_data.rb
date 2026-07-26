# Data.define の単方向リストを make_shareable、reader が全走査
# axes: len=300 readers=3 compacts=4 data linked
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
LNode = Data.define(:v, :nxt)
LEN = 300
head = nil
(LEN - 1).downto(0) { |i| head = LNode.new(v: i, nxt: head) }
LIST = Ractor.make_shareable(head)
EXP = (0...LEN).sum
rs = 3.times.map do |rid|
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
4.times { GC.compact }
rs.each { |ra| raise "mismatch" unless ra.value == EXP }

puts "OK i56_linked_list_data"
