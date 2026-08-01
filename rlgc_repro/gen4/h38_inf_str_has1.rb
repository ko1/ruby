# h38_inf_str_has1: (1..).lazy strings containing 1 first(k)
# axes: infinite-lazy, endless-range, copy, GC.start
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def calc_inf_str_has1
  (1..).lazy.map { |x| x.to_s }.select { |s| s.include?("1") }.first(6)
end
ref = calc_inf_str_has1
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  GC.start
  po.send(calc_inf_str_has1)
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h38_inf_str_has1"
