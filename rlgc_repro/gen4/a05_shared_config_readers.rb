# 共有 immutable config を全 worker が読む(make_shareable + 並行 read)
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
config = Ractor.make_shareable({
  db: { host: "h".freeze, port: 5432 }.freeze,
  flags: [1, 2, 3].freeze,
  name: "app".freeze,
}.freeze)
ws = 8.times.map do |i|
  Ractor.new(config, i) do |cfg, id|
    sum = 0
    500.times { sum += cfg[:db][:port] + cfg[:flags].sum + id; +"scratch#{sum}" }
    sum
  end
end
GC.compact
raise unless ws.map(&:value).all? { |v| v.is_a?(Integer) }
puts "OK a05"
