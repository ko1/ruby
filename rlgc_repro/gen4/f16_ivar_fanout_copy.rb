# f16 profile store: objects with 1..40 ivars (embedded->extended shapes), copy, full ivar audit
# axes: copy, ivar count sweep, GC.start every 10
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

class Profile
  def initialize(n)
    n.times { |i| instance_variable_set(:"@f#{i}", "val#{i}") }
  end
end

port = Ractor::Port.new
w = Ractor.new(port) do |po|
  loop do
    mm = Ractor.receive
    break if mm == :eof
    ivs = mm.instance_variables.sort
    vals_ok = ivs.each_with_index.all? { |nm, _| mm.instance_variable_get(nm) == "val#{nm.to_s[2..]}" }
    po.send([ivs.size, vals_ok])
  end
end

counts = STRESS ? [1, 7, 40] : (1..40).to_a
counts.each do |n|
  w.send(Profile.new(n))
  cnt, ok = port.receive
  assert cnt == n, "ivar count #{cnt} != #{n}"
  assert ok, "ivar values for n=#{n}"
  GC.start if n % 10 == 0
end
w.send(:eof)
puts "OK f16_ivar_fanout_copy"
