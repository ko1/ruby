# two star hubs sharing 6 spokes: spokes alternate hub A / hub B per message
# axes: dual hub totals, copy payload, GC.start at hub B mid-stream
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

K = 6
R = 4
hreg = Ractor::Port.new
hubs = 2.times.map do |h|
  Ractor.new(h, hreg, K * R / 2) do |hid, reg, total|
    inbox = Ractor::Port.new
    reg.send([hid, inbox])
    sum = 0
    total.times do |k|
      sum += inbox.receive
      GC.start if hid == 1 && k == total / 2
    end
    sum
  end
end
hub_in = Array.new(2)
2.times do
  hid, p = hreg.receive
  hub_in[hid] = p
end
done = Ractor::Port.new
spokes = K.times.map do |i|
  Ractor.new(i, hub_in[0], hub_in[1], done, R) do |id, ha, hb, dport, rounds|
    rounds.times do |r|
      (r.even? ? ha : hb).send(id * 100 + r)
    end
    dport.send(id)
    :fin
  end
end
K.times { done.receive }
GC.stress = false
sum_a = hubs[0].value
sum_b = hubs[1].value
exp_a = (0...K).sum { |i| [0, 2].sum { |r| i * 100 + r } }
exp_b = (0...K).sum { |i| [1, 3].sum { |r| i * 100 + r } }
raise "A #{sum_a} != #{exp_a}" unless sum_a == exp_a
raise "B #{sum_b} != #{exp_b}" unless sum_b == exp_b
spokes.each { |r| raise unless r.value == :fin }
puts "OK e21_star_two_hubs"
