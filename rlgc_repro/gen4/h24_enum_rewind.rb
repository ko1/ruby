# h24_enum_rewind: Enumerator.new + next/rewind/next
# axes: enumerator-external, next/take, copy, GC.start
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def mk_enum_rewind
  Enumerator.new { |y| i = 5; loop { y << i; i += 2 } }
end
def run_enum_rewind(n)
  e = mk_enum_rewind
  out = []
  3.times { out << e.next }; e.rewind; (n - 3).times { out << e.next }
  out
end
ref = run_enum_rewind(19)
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  GC.start
  po.send(run_enum_rewind(19))
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h24_enum_rewind"
