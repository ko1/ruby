# f18 session accretor: one object grows 1->40 ivars across sends (embedded->extended transition live)
# axes: copy each growth step, shape transition, GC.compact at boundary
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

class Session
end

port = Ractor::Port.new
w = Ractor.new(port) do |po|
  loop do
    mm = Ractor.receive
    break if mm == :eof
    po.send(mm.instance_variables.map { |nm| [nm, mm.instance_variable_get(nm)] }.sort)
  end
end

s = Session.new
steps = STRESS ? [0, 2, 5, 12, 39] : (0...40).to_a
last = -1
steps.each do |i|
  (last + 1).upto(i) { |j| s.instance_variable_set(:"@s#{j}", j * 7) }
  last = i
  w.send(s)
  back = port.receive
  assert back.size == i + 1, "step #{i}: got #{back.size} ivars"
  back.each { |nm, vv| assert vv == nm.to_s[2..].to_i * 7, "step #{i}: #{nm}=#{vv}" }
  GC.compact if i == 12 # around embedded->extended neighborhood
end
w.send(:eof)
puts "OK f18_embed_extend_growth"
