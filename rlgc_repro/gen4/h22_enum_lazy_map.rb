# h22_enum_lazy_map: Enumerator.new .lazy.map .first
# axes: enumerator-external, next/take, copy, GC.start
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def mk_enum_lazy_map
  Enumerator.new { |y| i = 0; loop { y << i; i += 1 } }
end
def run_enum_lazy_map(n)
  e = mk_enum_lazy_map
  out = []
  out = e.lazy.map { |x| x * x + 1 }.first(n)
  out
end
ref = run_enum_lazy_map(17)
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  GC.start
  po.send(run_enum_lazy_map(17))
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h22_enum_lazy_map"
