# f43 range oracle: int/float/char + endless/beginless ranges queried remotely
# axes: copy, Range leaves, GC.start
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

port = Ractor::Port.new
w = Ractor.new(port) do |po|
  loop do
    mm = Ractor.receive
    break if mm == :eof
    rng, probe = mm
    po.send([rng.cover?(probe), rng.begin, rng.end, rng.exclude_end?])
  end
end

cases = [
  [(1..10), 5, true, 1, 10, false],
  [(1...10), 10, false, 1, 10, true],
  [(2.5..3.5), 3.0, true, 2.5, 3.5, false],
  [('aa'..'az'), 'ak', true, 'aa', 'az', false],
  [(100..), 1_000_000, true, 100, nil, false],
  [(..0), -5, true, nil, 0, false],
  [(..0), 1, false, nil, 0, false],
]
cases.each { |rng, probe, _, _, _, _| w.send([rng, probe]) }
GC.start
cases.each do |rng, probe, want, wb, we, wx|
  cov, gb, ge, gx = port.receive
  assert cov == want, "#{rng.inspect}.cover?(#{probe.inspect}) => #{cov}"
  assert gb == wb && ge == we, "bounds of #{rng.inspect}"
  assert gx == wx, "exclude_end of #{rng.inspect}"
end
w.send(:eof)
puts "OK f43_range_query_service"
