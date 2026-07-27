# wave ごとに parser ractor を使い捨てる log 集計 (respawn + 各 wave 厳密検証)
# axes: 3 waves x 2 workers, respawn, copy, GC.compact between waves
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

port = Ractor::Port.new # wave 間で共有 (per-wave port は stress 下で join が固まる)
3.times do |w|
  nl = 12
  lines = Array.new(nl) { |i| "w#{w} op=#{(i + w) % 4} cost=#{i * 3 + w}" }
  exp_cost = (0...nl).sum { |i| i * 3 + w } # 算術で
  ws = 2.times.map do
    Ractor.new(port) do |o|
      c = 0
      loop do
        l = Ractor.receive
        break if l == :stop
        c += l[/cost=(\d+)/, 1].to_i
      end
      o.send(c)
    end
  end
  lines.each_with_index { |l, i| ws[i % 2].send(l) }
  ws.each { |r| r.send(:stop) }
  got = 0
  2.times { got += port.receive }
  ws.each(&:value)
  raise "wave#{w}: #{got} != #{exp_cost}" unless got == exp_cost
  GC.compact if w == 1
end
puts "OK b18_log_parse_waves"
