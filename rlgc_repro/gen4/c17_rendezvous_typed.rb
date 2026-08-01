# c17: typed rendezvous: producers meet consumers via matchmaker with two queues;
# items handed producer->consumer, acks consumer->producer. Multiset conserved.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

P = STRESS ? 3 : 6

match = Ractor.new(P) do |n|
  puts_q = []
  gets_q = []
  handed = 0
  (2 * n).times do
    msg = Ractor.receive
    case msg[0]
    when :put then puts_q << msg
    when :get then gets_q << msg
    else raise "tag"
    end
    while !puts_q.empty? && !gets_q.empty?
      _, pid, item, ackport = puts_q.shift
      _, _cid, cport = gets_q.shift
      cport << [:item, pid, item, ackport]
      handed += 1
    end
  end
  raise "leftover" unless puts_q.empty? && gets_q.empty?
  handed
end

done = Ractor::Port.new
prods = P.times.map do |i|
  Ractor.new(match, done, i) do |mm, dp, id|
    ack = Ractor::Port.new
    mm.send([:put, id, "item-#{id}", ack])
    tag, cid = ack.receive
    raise "ack" unless tag == :ack
    dp << [:pdone, id, cid]
  end
end
cons = P.times.map do |i|
  Ractor.new(match, done, i) do |mm, dp, id|
    my = Ractor::Port.new
    mm.send([:get, id, my])
    tag, pid, item, ackport = my.receive
    raise "item tag" unless tag == :item
    raise "item body" unless item == "item-#{pid}"
    ackport << [:ack, id]
    dp << [:cdone, id, pid]
  end
end

pids_at_cons = []
cids_at_prods = []
(2 * P).times do
  t, _id, other = done.receive
  case t
  when :pdone then cids_at_prods << other
  when :cdone then pids_at_cons << other
  else raise "done tag"
  end
end
raise "items" unless pids_at_cons.sort == (0...P).to_a
raise "acks" unless cids_at_prods.sort == (0...P).to_a
GC.stress = false
raise "handed" unless match.value == P
(prods + cons).each(&:value)
puts "OK c17_rendezvous_typed"
