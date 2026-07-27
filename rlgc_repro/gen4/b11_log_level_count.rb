# log 行を 3 worker に分配して level 別件数を集計 (worker 側で split parse)
# axes: 3 workers, copy batches, exact per-level counts (expected は算術で用意)
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

LEVELS = %w[DEBUG INFO WARN ERROR].freeze
N = 45
lines = Array.new(N) { |i| "t=#{i} level=#{LEVELS[(i * i) % 4]} msg=op#{i % 7}" }
expected = Hash.new(0)
N.times { |i| expected[LEVELS[(i * i) % 4]] += 1 } # parse せず算術で

out = Ractor::Port.new
ws = 3.times.map do
  Ractor.new(out) do |o|
    counts = Hash.new(0)
    loop do
      batch = Ractor.receive
      break if batch == :stop
      batch.each { |l| counts[l.split(" ")[1].split("=")[1]] += 1 }
    end
    o.send(counts)
  end
end
lines.each_slice(9).with_index { |sl, i| ws[i % 3].send(sl) }
ws.each { |w| w.send(:stop) }
merged = Hash.new(0)
3.times { out.receive.each { |k, v| merged[k] += v } }
ws.each(&:value)
raise "merged=#{merged}" unless merged == expected
raise "total" unless merged.values.sum == N
puts "OK b11_log_level_count"
