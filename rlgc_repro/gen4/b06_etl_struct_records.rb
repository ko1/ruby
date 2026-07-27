# Struct レコードを copy で流す ETL: parse -> enrich -> aggregate
# axes: 2 workers, Struct payload, copy
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

Rec = Struct.new(:id, :name, :qty, :total)
N = 24
out = Ractor::Port.new
ws = 2.times.map do |wid|
  Ractor.new(out, wid) do |o, id|
    loop do
      rec = Ractor.receive
      break if rec == :stop
      rec.total = rec.qty * (rec.name.bytesize + 1)
      o.send(rec)
    end
  end
end
expected = 0
N.times do |k|
  r = Rec.new(k, "prod-#{k % 9}", k % 13 + 1, 0)
  expected += r.qty * (r.name.bytesize + 1)
  ws[k % 2].send(r)
end
sum = 0
ids = 0
N.times do
  rec = out.receive
  sum += rec.total
  ids += rec.id
end
GC.compact
ws.each { |w| w.send(:stop) }
ws.each(&:value)
raise "sum=#{sum} exp=#{expected}" unless sum == expected
raise "ids" unless ids == (0...N).sum
puts "OK b06_etl_struct_records"
