# h35_inf_fib_lazy: Enumerator.new fib .lazy.first(k)
# axes: infinite-lazy, endless-range, copy, GC.start
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def calc_inf_fib_lazy
  Enumerator.new { |y| a, b = 0, 1; loop { y << a; a, b = b, a + b } }.lazy.first(10)
end
ref = calc_inf_fib_lazy
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  GC.start
  po.send(calc_inf_fib_lazy)
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h35_inf_fib_lazy"
