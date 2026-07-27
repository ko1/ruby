# bounce routing: request runs right until ttl exhausted, then reverses and returns to main
# axes: bidirectional wiring, direction flip at ttl boundary, visited path both ways
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 5
J = 3
home = Ractor::Port.new
nodes = N.times.map do |i|
  Ractor.new(i, J) do |id, jobs|
    tag, prv, nxt = Ractor.receive
    raise unless tag == :wire
    handled = 0
    expect = nil
    while expect.nil? || handled < expect
      m = Ractor.receive
      if m[0] == :quota
        expect = m[1]
        next
      end
      dir, ttl, path = m
      path << id
      if dir == :out && ttl <= 1
        prv.send([:back, 0, path])
      elsif dir == :out
        nxt.send([:out, ttl - 1, path])
      else
        prv.send([:back, 0, path])
      end
      handled += 1
    end
    :fin
  end
end
N.times do |i|
  prv = i == 0 ? home : nodes[i - 1]
  nxt = i == N - 1 ? nil : nodes[i + 1]
  nodes[i].send([:wire, prv, nxt])
end
# quota per node: outbound visits + return visits, fully determined by ttls
ttls = [2, 4, 3]
counts = Array.new(N, 0)
ttls.each do |t|
  turn = t - 1 # node index where direction flips
  (0..turn).each { |i| counts[i] += 1 }      # outbound pass
  (0...turn).each { |i| counts[i] += 1 }     # return pass
end
N.times { |i| nodes[i].send([:quota, counts[i]]) }
ttls.each_with_index { |t, k| nodes[0].send([:out, t, [k]]) }
J.times do
  _dir, _ttl, path = home.receive
  k = path[0]
  t = ttls[k]
  exp = [k] + (0...t).to_a + (0...(t - 1)).to_a.reverse
  raise "path #{path}" unless path == exp
end
GC.stress = false
nodes.each { |r| raise unless r.value == :fin }
puts "OK e70_ttl_bounce"
