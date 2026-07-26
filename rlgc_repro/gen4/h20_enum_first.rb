# h20_enum_first: Enumerator.new + first(k): +7 counter
# axes: enumerator-external, next/take, copy, GC.start
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def mk_enum_first
  Enumerator.new { |y| i = 0; loop { y << i + 7; i += 1 } }
end
def run_enum_first(n)
  e = mk_enum_first
  out = []
  out = e.first(n)
  out
end
ref = run_enum_first(15)
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  GC.start
  po.send(run_enum_first(15))
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h20_enum_first"
