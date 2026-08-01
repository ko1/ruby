# ETL fanout/fanin: main が抽出、3 transformer が変換、1 loader が集約 (loader へ直接 send)
# axes: 5 ractors, copy, exact record count で終了判定
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 30
NW = 3
loader = Ractor.new(N) do |n|
  total = 0
  n.times do
    rec = Ractor.receive
    total += rec[:len]
  end
  total
end
ws = NW.times.map do
  Ractor.new(loader) do |dst|
    loop do
      rec = Ractor.receive
      break if rec == :stop
      rec[:len] = rec[:line].split(",").size + rec[:line].bytesize
      dst.send(rec)
    end
  end
end
expected = 0
N.times do |k|
  ncols = k % 3 + 1
  line = "f#{k}," + "c," * ncols
  expected += (ncols + 1) + line.bytesize # split は末尾空フィールドを落とすので ncols+1
  ws[k % NW].send({ id: k, line: line, len: 0 })
end
ws.each { |w| w.send(:stop) }
ws.each(&:value)
total = loader.value
raise "total=#{total} exp=#{expected}" unless total == expected
puts "OK b04_etl_fanout_fanin"
