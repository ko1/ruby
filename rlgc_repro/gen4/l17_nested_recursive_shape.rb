# nested mixed structure copied; compact during window
# axes: copy, nested mixed, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
w = Ractor.new(port) do |o|
  node = Ractor.receive
  d = 0; n = node
  while n; d += 1; n = n[:next]; end
  o.send(d)
end
head = nil
100.times { |i| head = { val: i, next: head } }
w.send(head)
GC.compact
res = port.receive; w.value
raise "depth #{res}" unless res == 100
puts "OK l17_nested_recursive_shape"
