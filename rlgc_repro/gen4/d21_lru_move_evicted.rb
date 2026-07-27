# LRU cache where evicted entries are moved back to the client via port move.
# Axes: cap=4, 60 puts, move responses, stress in service.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
done = Ractor::Port.new
svc = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  h = {}
  loop do
    msg = Ractor.receive
    break if msg == :stop
    op, k, v, rp = msg
    case op
    when :put
      h.delete(k); h[k] = v
      if h.size > 4
        ek = h.first[0]
        ev = h.delete(ek)
        rp.send([:evicted, ek, ev], move: true)
      else
        rp << [:stored, nil, nil]
      end
    end
  end
  GC.stress = false
  done << :done
  h.keys
end
rp = Ractor::Port.new
evlog = []
60.times do |i|
  svc.send([:put, "k#{i}", +"payload-#{i}", rp])
  tag, ek, ev = rp.receive
  if i < 4
    raise unless tag == :stored
  else
    raise "ev#{i}" unless tag == :evicted && ek == "k#{i - 4}" && ev == "payload-#{i - 4}"
    evlog << ek
  end
end
raise unless evlog == (0..55).map { "k#{_1}" }
svc.send(:stop)
done.receive
raise unless svc.value == %w[k56 k57 k58 k59]
puts "OK d21_lru_move_evicted"
