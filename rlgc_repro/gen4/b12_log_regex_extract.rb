# worker 内で regex parse (MatchData は worker 内に閉じ、文字列だけ返す)
# axes: 2 workers, copy, regex capture -> new String
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 30
paths = ["/api/v0/item", "/api/v1/item", "/api/v2/item"].freeze
lines = Array.new(N) { |i| "[req-#{i}] path=#{paths[i % 3]} status=#{200 + (i % 4) * 100}" }
out = Ractor::Port.new
ws = 2.times.map do
  Ractor.new(out) do |o|
    loop do
      l = Ractor.receive
      break if l == :stop
      m = /path=(\S+) status=(\d+)/.match(l)
      o.send([String.new(m[1]), Integer(m[2])])
    end
  end
end
exp_status = 0
exp_paths = Hash.new(0)
N.times do |i|
  exp_status += 200 + (i % 4) * 100
  exp_paths[paths[i % 3]] += 1
  ws[i % 2].send(lines[i])
end
st_sum = 0
got_paths = Hash.new(0)
N.times do
  path, st = out.receive
  st_sum += st
  got_paths[path] += 1
end
ws.each { |w| w.send(:stop) }
ws.each(&:value)
raise "status" unless st_sum == exp_status
raise "paths" unless got_paths == exp_paths
puts "OK b12_log_regex_extract"
