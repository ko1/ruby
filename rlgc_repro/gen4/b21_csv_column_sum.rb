# CSV 行 "id,name,qty,price" を worker で型付けし qty*price を slice 単位で合計
# axes: 4 workers, copy slices, expected は算術で
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 40
NW = 4
rows = Array.new(N) { |i| "#{i},p#{i % 7},#{i % 9 + 1},#{(i * 37) % 500 + 100}" }
expected = (0...N).sum { |i| (i % 9 + 1) * ((i * 37) % 500 + 100) }

out = Ractor::Port.new
ws = NW.times.map do
  Ractor.new(out) do |o|
    loop do
      sl = Ractor.receive
      break if sl == :stop
      o.send(sl.sum { |r| c = r.split(","); Integer(c[2]) * Integer(c[3]) })
    end
  end
end
jobs = 0
rows.each_slice(8) do |sl|
  ws[jobs % NW].send(sl)
  jobs += 1
end
got = 0
jobs.times { got += out.receive }
ws.each { |w| w.send(:stop) }
ws.each(&:value)
raise "got=#{got} exp=#{expected}" unless got == expected
puts "OK b21_csv_column_sum"
