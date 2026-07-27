# 極小配列 sort を worker 側 GC.stress で回す (sort 内 allocation を stress 下で)
# axes: 2 workers, worker-side stress, tiny arrays
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

port = Ractor::Port.new
ws = 2.times.map do
  Ractor.new(port) do |o|
    GC.stress = true if ENV['S_STRESS']
    loop do
      msg = Ractor.receive
      break if msg == :stop
      i, a = msg
      o.send([i, a.sort])
    end
    GC.stress = false
  end
end
NJ = 6
jobs = Array.new(NJ) { |j| Array.new(8) { |i| (i * 29 + j * 7) % 53 } }
NJ.times { |j| ws[j % 2].send([j, jobs[j]]) }
got = Array.new(NJ)
NJ.times do
  i, a = port.receive
  got[i] = a
end
ws.each { |w| w.send(:stop) }
ws.each(&:value)
NJ.times { |j| raise "job#{j}" unless got[j] == jobs[j].sort }
puts "OK b45_sort_worker_stress"
