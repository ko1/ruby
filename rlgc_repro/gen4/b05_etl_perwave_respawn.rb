# wave ごとに transform worker 群を作り直す ETL (per-wave respawn)
# axes: 3 workers x 5 waves, copy, GC.start between waves
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

WAVES = 3
NW = 2
PER = 8
port = Ractor::Port.new # wave 跨ぎで共有 (wave ごとの新 port は join hang: scratchpad/min1.rb)
WAVES.times do |w|
  ws = NW.times.map do |wid|
    Ractor.new(port, wid) do |o, id|
      loop do
        job = Ractor.receive
        break if job == :stop
        o.send(job.sum { |v| v * v })
      end
    end
  end
  expected = 0
  PER.times do |k|
    vals = Array.new(6) { |i| w * 100 + k * 10 + i }
    expected += vals.sum { |v| v * v }
    ws[k % NW].send(vals)
  end
  got = 0
  PER.times { got += port.receive }
  ws.each { |r| r.send(:stop) }
  ws.each(&:value)
  raise "wave#{w}: got=#{got} exp=#{expected}" unless got == expected
  GC.start if w.odd?
end
puts "OK b05_etl_perwave_respawn"
