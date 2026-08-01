# log 行を Struct にパースして返送し、field ごとの合計を検証
# axes: 3 workers, Struct return payload, copy
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

LogRec = Struct.new(:t, :lvl, :code)
N = 24
out = Ractor::Port.new
ws = 3.times.map do
  Ractor.new(out) do |o|
    loop do
      l = Ractor.receive
      break if l == :stop
      t, lvl, code = l.split("|").map { |x| Integer(x) }
      o.send(LogRec.new(t, lvl, code))
    end
  end
end
N.times { |i| ws[i % 3].send("#{i * 2}|#{i % 4}|#{(i * 5) % 17}") }
tsum = lsum = csum = 0
N.times do
  r = out.receive
  tsum += r.t
  lsum += r.lvl
  csum += r.code
end
ws.each { |w| w.send(:stop) }
ws.each(&:value)
raise "t" unless tsum == (0...N).sum { |i| i * 2 }
raise "l" unless lsum == (0...N).sum { |i| i % 4 }
raise "c" unless csum == (0...N).sum { |i| (i * 5) % 17 }
puts "OK b19_log_struct_fields"
