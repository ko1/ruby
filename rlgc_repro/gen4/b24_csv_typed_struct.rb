# CSV -> Struct 型付け変換を worker で行い、型と値を main で検証
# axes: 2 workers, Struct payload return, copy, GC.start mid-stream
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

Item = Struct.new(:id, :name, :qty)
N = 24
NAMES = Array.new(5) { |i| "nm#{i}" }.freeze
out = Ractor::Port.new
ws = 2.times.map do
  Ractor.new(out) do |o|
    loop do
      row = Ractor.receive
      break if row == :stop
      id, name, qty = row.split(",")
      o.send(Item.new(Integer(id), name, Integer(qty)))
    end
  end
end
N.times do |i|
  ws[i % 2].send("#{i},#{NAMES[i % 5]},#{i * 4}")
  GC.start if i == N / 2
end
seen = {}
N.times do
  it = out.receive
  raise "type" unless it.is_a?(Item) && it.name.is_a?(String)
  seen[it.id] = [it.name, it.qty]
end
ws.each { |w| w.send(:stop) }
ws.each(&:value)
raise "size" unless seen.size == N
N.times do |i|
  raise "row#{i}" unless seen[i][0] == NAMES[i % 5] && seen[i][1] == i * 4
end
puts "OK b24_csv_typed_struct"
