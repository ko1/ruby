# 成功/例外の混在、ensure で port にタリー、GC 挟み(mod 2)
# axes: mixed,ensure,compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
N = 12
MOD = 2
port = Ractor::Port.new
workers = N.times.map do |k|
  Ractor.new(port, k) do |pt, id|
    Thread.current.report_on_exception = false
    begin
      data = Array.new(8) { +"m-#{id}-#{_1}" }
      raise "fail-#{id}" if (id % MOD).zero?
      pt.send([id, :ok])
      data.sum(&:bytesize)
    ensure
      pt.send([id, :ensure])
    end
  end
end
fails = 0
workers.each_with_index do |w, k|
  begin
    w.value
  rescue Ractor::RemoteError
    fails += 1
  end
  GC.compact if k == 6
end
expected_fail = (0...N).count { |x| (x % MOD).zero? }
successes = N - expected_fail
total = N + successes  # ensure marker per worker + ok marker per success
ensures = 0
oks = 0
total.times do
  m = port.receive
  ensures += 1 if m[1] == :ensure
  oks += 1 if m[1] == :ok
end
raise "ensures" unless ensures == N
raise "oks" unless oks == successes
raise "fails" unless fails == expected_fail
puts "OK k64_mixed_compact_between"
