# round1 で単語頻度、round2 は頻度 >= 3 の単語のみ respawn worker が再カウントし一致検証
# axes: 2 rounds x 2 workers respawn, copy, shareable 中間結果
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

WORDS = %w[red blue lime teal pink onyx].freeze
N = 54
stream = Array.new(N) { |i| WORDS[(i * i + 2 * i) % 6] }
freq = Hash.new(0)
stream.each { |w| freq[w] += 1 }

r1 = Ractor::Port.new
w1 = 2.times.map do |wi|
  Ractor.new(r1, stream.each_with_index.select { |_, i| i % 2 == wi }.map(&:first)) do |o, part|
    h = Hash.new(0)
    part.each { |w| h[w] += 1 }
    o.send(h)
  end
end
got1 = Hash.new(0)
2.times { r1.receive.each { |k, v| got1[k] += v } }
w1.each(&:value)
raise "round1" unless got1 == freq

HOT = Ractor.make_shareable(freq.select { |_, c| c >= 3 }.keys.sort)
r2 = Ractor::Port.new
w2 = 2.times.map do |wi|
  Ractor.new(r2, stream.each_with_index.select { |_, i| i % 2 == wi }.map(&:first), HOT) do |o, part, hot|
    h = Hash.new(0)
    part.each { |w| h[w] += 1 if hot.include?(w) }
    o.send(h)
  end
end
got2 = Hash.new(0)
2.times { r2.receive.each { |k, v| got2[k] += v } }
w2.each(&:value)
exp2 = freq.select { |k, c| c >= 3 }
raise "round2 #{got2} != #{exp2}" unless got2 == exp2
puts "OK b79_wordcount_two_rounds"
