# h68_move_fib: move enumerator result: fib first(r)
# axes: enumerator-result, move:true, GC.compact, request/response
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def gen_move_fib(req)
  Enumerator.new { |y| a, b = 0, 1; loop { y << a; a, b = b, a + b } }.first(req)
end
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  loop do
    req = Ractor.receive
    break if req == :stop
    arr = gen_move_fib(req)
    po.send(arr, move: true)
  end
  :done
end
rounds = 5
rounds.times do |k|
  req = k + 4
  w.send(req)
  arr = port.receive
  ref = gen_move_fib(req)
  raise "mismatch: #{arr.inspect} != #{ref.inspect}" unless arr == ref
  GC.compact
end
w.send(:stop)
raise "join" unless w.value == :done
puts "OK h68_move_fib"
