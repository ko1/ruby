PORTS = 16
collect = Ractor::Port.new

receivers = PORTS.times.map do |i|
  Ractor.new(i, collect) do |id, cport|
    port = Ractor::Port.new
    cport.send([id, port])
    total = 0
    loop do
      msg = port.receive
      break if msg == :stop
      msg.each { |h| total += h[:n] + h[:s].bytesize + h[:deep][1].bytesize }
    end
    total
  end
end

ports = {}
PORTS.times { id, port = collect.receive; ports[id] = port }

def deep_graph(seed)
  Array.new(40) do |j|
    { n: seed + j,
      s: +"payload-#{seed}-#{j}-#{'q' * 50}",
      deep: [j, +"inner-#{seed}-#{j}-#{'z' * 30}", [j, j + 1, [seed, j]]] }
  end
end

senders = 8.times.map do |t|
  Thread.new(t) do |tn|
    600.times do |k|
      ports.each_value { |p| p.send(deep_graph(tn * 100000 + k)) }  # COPY send
    end
  end
end

senders.each(&:join)
ports.each_value { |p| p.send(:stop) }
sums = receivers.map(&:value)
GC.start(full_mark: true)
puts "ok sums=#{sums.sum}"