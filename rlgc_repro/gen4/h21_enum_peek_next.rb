# h21_enum_peek_next: Enumerator.new + peek/next interleave
# axes: enumerator-external, next/take, copy, GC.start
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def mk_enum_peek_next
  Enumerator.new { |y| i = 1; loop { y << i; i += 1 } }
end
def run_enum_peek_next(n)
  e = mk_enum_peek_next
  out = []
  n.times { out << e.peek; out << e.next }
  out
end
ref = run_enum_peek_next(16)
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  GC.start
  po.send(run_enum_peek_next(16))
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h21_enum_peek_next"
