# Session cache storing ivar'd Session objects; copies validated field-by-field.
# Axes: 1 service, 90 sessions cap 20, ivar payloads, stress in service.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
class Session
  attr_reader :uid, :cart, :meta
  def initialize(uid, cart, meta) = (@uid = uid; @cart = cart; @meta = meta)
end
done = Ractor::Port.new
svc = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  h = {}
  loop do
    msg = Ractor.receive
    break if msg == :stop
    op, k, sess, rp = msg
    case op
    when :put
      h.delete(k); h[k] = sess
      h.delete(h.first[0]) if h.size > 20
      rp << h.size
    when :get then rp << h[k]
    end
  end
  GC.stress = false
  done << :done
  h.size
end
rp = Ractor::Port.new
90.times do |i|
  s = Session.new("u#{i}", ["item#{i}", "item#{i + 1}"], { ts: i })
  svc.send([:put, "u#{i}", s, rp])
  raise unless rp.receive == [i + 1, 20].min
end
# last 20 sessions survive (u70..u89)
70.upto(89) do |i|
  svc.send([:get, "u#{i}", nil, rp])
  s = rp.receive
  raise "sess#{i}" unless s.is_a?(Session) && s.uid == "u#{i}" && s.cart == ["item#{i}", "item#{i + 1}"] && s.meta == { ts: i }
end
svc.send([:get, "u0", nil, rp])
raise "evicted still present" unless rp.receive.nil?
svc.send(:stop)
done.receive
raise unless svc.value == 20
puts "OK d20_sess_ivar_objects"
