# bidirectional chain of 5: requests flow right, tail reflects, replies flow left to main
# axes: prev/next wiring via first tagged message, both directions counted per node
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 5
J = 4
done = Ractor::Port.new
nodes = N.times.map do |i|
  Ractor.new(i, N, J) do |id, n, jobs|
    tag, prv, nxt = Ractor.receive
    raise unless tag == :wire
    fwd = 0
    back = 0
    last = id == n - 1
    while fwd < jobs || (!last && back < jobs)
      dir, v = Ractor.receive
      if dir == :fwd
        fwd += 1
        if last
          prv.send([:back, v + 1000])
        else
          nxt.send([:fwd, v + 1])
        end
      else
        back += 1
        prv.send([:back, v + 1])
      end
    end
    :fin
  end
end
back_port = Ractor::Port.new
N.times do |i|
  prv = i == 0 ? back_port : nodes[i - 1]
  nxt = i == N - 1 ? nil : nodes[i + 1]
  nodes[i].send([:wire, prv, nxt])
end
J.times { |k| nodes[0].send([:fwd, k * 10]) }
got = []
J.times do
  dir, v = back_port.receive
  raise unless dir == :back
  got << v
end
exp = (0...J).map { |k| k * 10 + (N - 1) + 1000 + (N - 1) }
raise "got #{got.sort} exp #{exp.sort}" unless got.sort == exp.sort
GC.stress = false
nodes.each { |r| raise unless r.value == :fin }
puts "OK e30_chain_echo_back"
