# h70_move_partition: move enumerator result: partition even
# axes: enumerator-result, move:true, GC.compact, request/response
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def gen_move_partition(req)
  (1..req).partition { |x| x.even? }
end
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  loop do
    req = Ractor.receive
    break if req == :stop
    arr = gen_move_partition(req)
    po.send(arr, move: true)
  end
  :done
end
rounds = 5
rounds.times do |k|
  req = k + 4
  w.send(req)
  arr = port.receive
  ref = gen_move_partition(req)
  raise "mismatch: #{arr.inspect} != #{ref.inspect}" unless arr == ref
  GC.compact
end
w.send(:stop)
raise "join" unless w.value == :done
puts "OK h70_move_partition"
