# content-based routing by payload class: Integer/String/Array dispatched to 3 typed workers
# axes: class-based dispatch, mixed payloads, per-type digests
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

reg = Ractor::Port.new
done = Ractor::Port.new
workers = 3.times.map do |w|
  Ractor.new(w, reg, done) do |wid, regp, dport|
    inbox = Ractor::Port.new
    regp.send([wid, inbox])
    digest = 0
    loop do
      m = inbox.receive
      break if m == :stop
      digest += case m
                when Integer then m
                when String then m.length * 100
                when Array then m.sum * 10000
                else raise "bad type"
                end
    end
    dport.send([wid, digest])
    :fin
  end
end
wports = Array.new(3)
3.times do
  wid, p = reg.receive
  wports[wid] = p
end
router = Ractor.new(wports) do |wp|
  loop do
    m = Ractor.receive
    if m == :stop
      wp.each { |p| p.send(:stop) }
      break
    end
    idx = case m
          when Integer then 0
          when String then 1
          when Array then 2
          end
    wp[idx].send(m)
  end
  :fin
end
msgs = [7, "abc", [1, 2], 11, "defgh", [3, 4, 5], 2, "x"]
msgs.each { |m| router.send(m) }
router.send(:stop)
exp = [7 + 11 + 2, (3 + 5 + 1) * 100, (3 + 12) * 10000]
3.times do
  wid, digest = done.receive
  raise "worker #{wid}: #{digest} != #{exp[wid]}" unless digest == exp[wid]
end
GC.stress = false
raise unless router.value == :fin
workers.each { |r| raise unless r.value == :fin }
puts "OK e46_route_bytype"
