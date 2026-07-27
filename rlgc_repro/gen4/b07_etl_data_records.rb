# Data.define レコードを move で流す ETL (immutable record + move 転送)
# axes: 4 workers, Data payload, move
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

Ev = Data.define(:seq, :tag, :vals)
N = 20
NW = 4
out = Ractor::Port.new
ws = NW.times.map do
  Ractor.new(out) do |o|
    loop do
      ev = Ractor.receive
      break if ev == :stop
      o.send([ev.seq, ev.vals.sum + ev.tag.bytesize])
    end
  end
end
expected = {}
N.times do |k|
  vals = [k * 7, k * 7 + 1, k * 7 + 2]
  tag = "tag-#{k % 11}"
  expected[k] = vals.sum + tag.bytesize
  ev = Ev.new(seq: k, tag: tag, vals: vals)
  ws[k % NW].send(ev, move: true)
end
got = {}
N.times do
  seq, v = out.receive
  got[seq] = v
end
ws.each { |w| w.send(:stop) }
ws.each(&:value)
raise "size" unless got.size == N
got.each { |k, v| raise "seq#{k}: #{v} != #{expected[k]}" unless v == expected[k] }
puts "OK b07_etl_data_records"
