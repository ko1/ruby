# throw/catch で制御フロー・決定的な結果を Port へ返す
# axes: throw,catch,value
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
N = 12
port = Ractor::Port.new
workers = N.times.map do |k|
  Ractor.new(port, k) do |pt, id|
    r = catch(:found) do
      acc = 0
      100.times do |j|
        acc += j
        throw(:found, [id, acc]) if acc >= (id + 1) * 10
      end
      [id, -1]
    end
    pt.send(r)
    :done
  end
end
workers.each(&:value)
got = N.times.map { port.receive }.sort_by(&:first)
GC.compact
raise "ids" unless got.map(&:first) == (0...N).to_a
raise "vals" unless got.all? { |(_id, v)| v >= 0 }
puts "OK k22_throwcatch_value_pass"
