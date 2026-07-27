# 木構造: 親 Ractor が子を生み、子が孫を生む。結果を value で畳み上げる
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
make_node = nil
make_node = ->(depth, id) do
  Ractor.new(depth, id) do |d, x|
    if d == 0
      x
    else
      # 子2つ(このブロック内で再帰生成はできないので値受信で代替)
      a = Ractor.receive
      b = Ractor.receive
      a + b + x
    end
  end
end
8.times do
  # 深さ2の二分木を手で
  leaves = 4.times.map { |i| Ractor.new(i) { |x| x } }
  mids = 2.times.map do |i|
    m = Ractor.new(100+i) { |x| Ractor.receive + Ractor.receive + x }
    m.send(leaves[i*2].value); m.send(leaves[i*2+1].value)
    m
  end
  root = Ractor.new(1000) { |x| Ractor.receive + Ractor.receive + x }
  root.send(mids[0].value); root.send(mids[1].value)
  raise unless root.value == (0+1+100) + (2+3+101) + 1000
  GC.compact
end
puts "OK a12"
