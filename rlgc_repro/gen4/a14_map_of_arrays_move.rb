# ネストした mutable graph を move: {k => [str,...]} を worker が変換
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
12.times do |round|
  w = Ractor.new do
    g = Ractor.receive
    g.transform_values { |arr| arr.map(&:upcase) }
  end
  graph = {}
  20.times { |i| graph["k#{i}"] = Array.new(5) { +"v#{i}-#{_1}" } }
  w.send(graph, move: true)
  res = w.value
  raise unless res["k0"][0] == "V0-0"
  GC.compact
end
puts "OK a14"
