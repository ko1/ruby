# Move in both directions: 2 client ractors move request bodies to a transform
# service, which moves transformed bodies back to each client's port.
# Axes: 2 clients x 30 reqs, move req+resp, stress in service and clients.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
done = Ractor::Port.new
svc = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  n = 0
  loop do
    msg = Ractor.receive
    break if msg == :stop
    body, rp = msg
    n += 1
    body[:words].map! { |w| w.upcase! || w }
    body[:len] = body[:words].sum(&:size)
    rp.send(body, move: true)
  end
  GC.stress = false
  done << :done
  n
end
clients = 2.times.map do |ci|
  Ractor.new(svc, ci, done, STRESS) do |svc, ci, done, stress|
    GC.stress = true if stress
    my = Ractor::Port.new
    total_len = 0
    30.times do |i|
      words = Array.new(4) { |j| +"w#{ci}x#{i}y#{j}" }
      svc.send([{ words: words, id: i }, my], move: true)
      back = my.receive
      raise "id" unless back[:id] == i
      raise "words" unless back[:words] == Array.new(4) { |j| "W#{ci}X#{i}Y#{j}" }
      total_len += back[:len]
    end
    GC.stress = false
    done << :cdone
    total_len
  end
end
2.times { raise unless done.receive == :cdone }
lens = clients.map(&:value)
want = 2.times.map { |ci| 30.times.sum { |i| 4.times.sum { |j| "w#{ci}x#{i}y#{j}".size } } }
raise "lens #{lens}" unless lens == want
svc.send(:stop)
done.receive
raise unless svc.value == 60
puts "OK d79_move_both_ways"
