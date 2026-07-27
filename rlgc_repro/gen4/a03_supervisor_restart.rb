# supervisor: worker が例外死したら作り直す(終了 Ractor value + 再生成)
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
spawn = -> (id) do
  Ractor.new(id) do |x|
    n = Ractor.receive
    raise "boom#{x}" if n.negative?
    n * x
  end
end
results = []
30.times do |i|
  w = spawn.call(i)
  w.send(i.even? ? i : -1)
  begin
    results << w.value
  rescue Ractor::RemoteError
    w2 = spawn.call(i)     # restart
    w2.send(i)
    results << w2.value
  end
  GC.compact if i % 4 == 0
end
raise unless results.size == 30
puts "OK a03"
