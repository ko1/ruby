# Ractor-local storage を使う worker 群 + 終了 + churn(local storage 解放経路)
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
KEY = :counter
2.times do |gen|
  ws = 6.times.map do |i|
    Ractor.new(i) do |id|
      Ractor.current[:counter] = 0
      100.times { Ractor.current[:counter] += 1; +"s#{id}-#{_1}" }
      Ractor.current[:counter]
    end
  end
  raise unless ws.map(&:value).all? { |v| v == 100 }
  GC.compact
  GC.start
end
puts "OK a15"
