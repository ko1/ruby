# h08_lazy_div5: lazy: multiples of 5 then /5, first(k)
# axes: lazy-chain, copy, GC.start, single worker
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
ref = (1..44).select { |x| x % 5 == 0 }.map { |x| x / 5 }.first(5)
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  res = (1..44).lazy.select { |x| x % 5 == 0 }.map { |x| x / 5 }.first(5)
  GC.start
  po.send(res)
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h08_lazy_div5"
