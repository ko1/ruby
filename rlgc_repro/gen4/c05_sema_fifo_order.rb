# c05: K=1 semaphore FIFO fairness; clients sequence requests via a baton ring
# (sema acks :queued before baton passes) so grant order is exactly 0..M-1.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

M = STRESS ? 4 : 8

sema = Ractor.new do
  locked = false
  waiting = []
  order = []
  loop do
    msg = Ractor.receive
    case msg[0]
    when :acquire
      msg[2] << :queued
      if locked
        waiting << [msg[1], msg[2]]
      else
        locked = true
        order << msg[1]
        msg[2] << :grant
      end
    when :release
      raise "not locked" unless locked
      if (pair = waiting.shift)
        order << pair[0]
        pair[1] << :grant
      else
        locked = false
      end
    when :stop
      raise "locked" if locked
      break order
    end
  end
end

reg = Ractor::Port.new
finish = Ractor::Port.new
clients = M.times.map do |i|
  Ractor.new(sema, reg, finish, i, M) do |s, rp, fin, id, m|
    my = Ractor::Port.new
    rp << [id, my]
    ports = my.receive          # ring table: array of ports
    baton = my.receive          # wait my turn to enqueue
    raise "baton" unless baton == :baton
    reply = Ractor::Port.new
    s.send([:acquire, id, reply])
    raise "q" unless reply.receive == :queued
    if id + 1 < m
      ports[id + 1] << :baton
    else
      fin << :all_queued
    end
    raise "g" unless reply.receive == :grant
    s.send([:release])
    fin << [:done, id]
    id
  end
end

ports = Array.new(M)
M.times do
  id, port = reg.receive
  ports[id] = port
end
clients.each_index { |i| ports[i] << ports }
ports[0] << :baton
got_all_queued = false
cnt = 0
(M + 1).times do
  m = finish.receive
  if m == :all_queued
    got_all_queued = true
  else
    raise "done" unless m[0] == :done
    cnt += 1
  end
end
raise "missing" unless got_all_queued && cnt == M
sema.send([:stop])
GC.stress = false
order = sema.value
raise "order #{order.inspect}" unless order == (0...M).to_a
clients.each(&:value)
GC.compact unless STRESS
puts "OK c05_sema_fifo_order"
