# h36_inf_primes: (2..).lazy primes first(k)
# axes: infinite-lazy, endless-range, copy, GC.start
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def calc_inf_primes
  (2..).lazy.select { |x| (2...x).none? { |d| x % d == 0 } }.first(8)
end
ref = calc_inf_primes
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  GC.start
  po.send(calc_inf_primes)
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h36_inf_primes"
