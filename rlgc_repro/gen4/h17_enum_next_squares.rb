# h17_enum_next_squares: Enumerator.new + .next: squares
# axes: enumerator-external, next/take, copy, GC.start
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def mk_enum_next_squares
  Enumerator.new { |y| i = 0; loop { y << i * i; i += 1 } }
end
def run_enum_next_squares(n)
  e = mk_enum_next_squares
  out = []
  n.times { out << e.next }
  out
end
ref = run_enum_next_squares(12)
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  GC.start
  po.send(run_enum_next_squares(12))
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h17_enum_next_squares"
