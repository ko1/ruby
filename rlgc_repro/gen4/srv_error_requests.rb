# gen4 server-sim: some requests are malformed; handlers rescue and answer
# error responses; one request class is FATAL and kills its handler — the
# dispatcher respawns the shard and the client retries.
# axes: transfer=copy, GC=none, exceptions=rescued in handler + handler death/respawn
N_REQS = 200

mk_handler = lambda do |gen|
  Ractor.new(gen) do |g|
    Thread.current.report_on_exception = false
    served = 0
    while (req = Ractor.receive) != :shutdown
      begin
        raise "fatal" if req[:kind] == :fatal && g == 0   # gen 0 vulnerable
        val =
          case req[:kind]
          when :ok    then req[:a] + req[:b]
          when :bad   then Integer(req[:a])               # raises on junk
          when :fatal then req[:a] + req[:b]              # survivable on gen>=1
          end
        served += 1
        req[:reply] << [:ok, req[:seq], val]
      rescue TypeError, ArgumentError
        served += 1
        req[:reply] << [:err, req[:seq]]
      end
    end
    served
  end
end

reply = Ractor::Port.new
handler = mk_handler.call(0)
gen = 0
served_total = 0
deaths = 0

exp_ok = exp_err = 0
N_REQS.times do |i|
  req =
    case i % 10
    when 7 then { seq: i, kind: :bad, a: "junk#{i}", reply: reply }
    when 9 then { seq: i, kind: :fatal, a: i, b: 1, reply: reply }
    else        { seq: i, kind: :ok, a: i, b: i * 2, reply: reply }
    end
  if i % 10 == 7 then exp_err += 1 else exp_ok += 1 end
  loop do
    handler << req
    if req[:kind] == :fatal && gen == 0
      begin
        handler.join
      rescue Ractor::RemoteError
        deaths += 1  # gen0 served counts die with it (requests 0..8)
      end
      gen += 1
      handler = mk_handler.call(gen)
      next # retry on the respawned handler
    end
    break
  end
end
handler << :shutdown
served_total += handler.value

got_ok = got_err = 0
N_REQS.times do
  tag, = reply.receive
  tag == :ok ? got_ok += 1 : got_err += 1
end
raise "FAIL deaths #{deaths}" unless deaths == 1  # only gen 0 is vulnerable
raise "FAIL ok #{got_ok}/#{exp_ok}" unless got_ok == exp_ok
raise "FAIL err" unless got_err == exp_err
# gen0 died at seq 9, so its 9 served requests are not in any surviving count
raise "FAIL served #{served_total}" unless served_total == N_REQS - 9
puts "OK srv_error_requests"
