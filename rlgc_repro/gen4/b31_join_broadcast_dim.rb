# 小さい dim 表を make_shareable で broadcast し fact 行を hash join
# axes: 3 workers, shareable dim + copy facts, join 結果は件数と合計で検証
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

DIM = Ractor.make_shareable((0...8).to_h { |k| [k, "name#{k}"] })
N = 36
exp_cnt = 0
exp_sum = 0
N.times do |i|
  k = (i * 5) % 11 # 8..10 は dim に無い
  if k < 8
    exp_cnt += 1
    exp_sum += DIM[k].bytesize + i
  end
end

out = Ractor::Port.new
ws = 3.times.map do
  Ractor.new(out, DIM) do |o, dim|
    cnt = 0
    sum = 0
    loop do
      msg = Ractor.receive
      break if msg == :stop
      k, v = msg
      if (nm = dim[k])
        cnt += 1
        sum += nm.bytesize + v
      end
    end
    o.send([cnt, sum])
  end
end
N.times { |i| ws[i % 3].send([(i * 5) % 11, i]) }
ws.each { |w| w.send(:stop) }
cnt = sum = 0
3.times do
  c, s = out.receive
  cnt += c
  sum += s
end
ws.each(&:value)
raise "cnt=#{cnt}" unless cnt == exp_cnt
raise "sum=#{sum}" unless sum == exp_sum
puts "OK b31_join_broadcast_dim"
