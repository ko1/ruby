# f72 dispatcher: round-robin over 3 long-lived workers, results via one shared port
# axes: copy, pool lifecycle, tagged fan-in ordering, GC.start mid-stream
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

port = Ractor::Port.new
pool = 3.times.map do |wi|
  Ractor.new(port, wi) do |po, myid|
    loop do
      mm = Ractor.receive
      break if mm == :eof
      jid, text = mm
      po.send([jid, myid, text.reverse])
    end
  end
end

n = STRESS ? 6 : 18
n.times { |i| pool[i % 3].send([i, "job-payload-#{i}"]) }
GC.start
seen = {}
n.times do
  jid, wid, rev = port.receive
  assert wid == jid % 3, "job #{jid} answered by worker #{wid}"
  assert rev == "job-payload-#{jid}".reverse, "job #{jid} result"
  seen[jid] = true
end
assert seen.size == n, "all jobs answered"
pool.each { |w| w.send(:eof) }
puts "OK f72_pool_roundrobin_ports"
