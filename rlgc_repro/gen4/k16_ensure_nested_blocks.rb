# ensure ブロックが die/finish 前に port へマーカー送出(nested)
# axes: ensure,side,nested
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
N = 12
port = Ractor::Port.new
workers = N.times.map do |k|
  Ractor.new(port, k) do |pt, id|
    Thread.current.report_on_exception = false
    begin
      tmp = Array.new(8) { +"e-#{id}-#{_1}" }
      raise "boom-#{id}" if id.odd?
      tmp.size
    ensure
      pt.send([id, :done])
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
  GC.compact if k == 5
end
markers = N.times.map { port.receive }
raise "markers" unless markers.map(&:first).sort == (0...N).to_a
raise "allmarked" unless markers.all? { |m| m[1] == :done }
raise 'fails' unless fails == N / 2
puts "OK k16_ensure_nested_blocks"
