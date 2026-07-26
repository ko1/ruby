# h19_enum_take: Enumerator.new + take(k): step by 3
# axes: enumerator-external, next/take, copy, GC.start
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def mk_enum_take
  Enumerator.new { |y| i = 0; loop { y << i * 3; i += 1 } }
end
def run_enum_take(n)
  e = mk_enum_take
  out = []
  out = e.take(n)
  out
end
ref = run_enum_take(14)
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  GC.start
  po.send(run_enum_take(14))
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h19_enum_take"
