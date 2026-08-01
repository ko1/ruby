# h65_move_lazy_sqr: move enumerator result: (1..r).lazy.map sqr to_a
# axes: enumerator-result, move:true, GC.compact, request/response
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def gen_move_lazy_sqr(req)
  (1..req).lazy.map { |x| x * x }.to_a
end
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  loop do
    req = Ractor.receive
    break if req == :stop
    arr = gen_move_lazy_sqr(req)
    po.send(arr, move: true)
  end
  :done
end
rounds = 5
rounds.times do |k|
  req = k + 4
  w.send(req)
  arr = port.receive
  ref = gen_move_lazy_sqr(req)
  raise "mismatch: #{arr.inspect} != #{ref.inspect}" unless arr == ref
  GC.compact
end
w.send(:stop)
raise "join" unless w.value == :done
puts "OK h65_move_lazy_sqr"
