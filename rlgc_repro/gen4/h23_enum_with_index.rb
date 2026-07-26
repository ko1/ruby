# h23_enum_with_index: Enumerator.new + with_index collect
# axes: enumerator-external, next/take, copy, GC.start
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def mk_enum_with_index
  Enumerator.new { |y| i = 0; loop { y << i * 2; i += 1 } }
end
def run_enum_with_index(n)
  e = mk_enum_with_index
  out = []
  e.take(n).each_with_index { |v, idx| out << [idx, v] }
  out
end
ref = run_enum_with_index(18)
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  GC.start
  po.send(run_enum_with_index(18))
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h23_enum_with_index"
