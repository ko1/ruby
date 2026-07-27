# Graceful shutdown protocol focus: service drains queue on :stop, acks via done
# port (stress already off), then exposes full final state through #value.
# Axes: 3 services, 60 ops each, copy, stress in services.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
done = Ractor::Port.new
svcs = 3.times.map do |si|
  Ractor.new(si, done, STRESS) do |si, done, stress|
    GC.stress = true if stress
    state = { id: si, ops: 0, sum: 0, last: nil }
    loop do
      msg = Ractor.receive
      break if msg == :stop
      v = msg
      state[:ops] += 1
      state[:sum] += v
      state[:last] = v
    end
    GC.stress = false
    done << [:bye, si, state[:ops]]
    state
  end
end
60.times { |i| svcs.each { |s| s.send(i * 2) } }
svcs.each { |s| s.send(:stop) }
byes = 3.times.map { done.receive }
raise "byes" unless byes.map { _1[1] }.sort == [0, 1, 2] && byes.all? { _1[0] == :bye && _1[2] == 60 }
svcs.each_with_index do |s, si|
  st = s.value
  raise "final#{si}" unless st == { id: si, ops: 60, sum: 60.times.sum { _1 * 2 }, last: 118 }
end
puts "OK d73_stop_value_final_state"
