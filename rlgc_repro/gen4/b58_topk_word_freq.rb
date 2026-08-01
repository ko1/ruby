# 単語頻度 top-3 を先頭文字 partition の 3 worker で数え、merge して厳密比較
# axes: 3 workers (disjoint words), copy, tie-break 辞書順
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

WORDS = %w[ant bee cat dog elk fox gnu hen ibis jay koi lark].freeze
N = 72
stream = Array.new(N) { |i| WORDS[(i * i) % 12] }
freq = Hash.new(0)
stream.each { |w| freq[w] += 1 }
exp_top = freq.sort_by { |w, c| [-c, w] }.first(3)

out = Ractor::Port.new
ws = 3.times.map do
  Ractor.new(out) do |o|
    h = Hash.new(0)
    loop do
      w = Ractor.receive
      break if w == :stop
      h[w] += 1
    end
    o.send(h)
  end
end
stream.each { |w| ws[w.bytes[0] % 3].send(w) }
ws.each { |w| w.send(:stop) }
merged = {}
3.times do
  out.receive.each do |k, v|
    raise "dup #{k}" if merged.key?(k)
    merged[k] = v
  end
end
ws.each(&:value)
top = merged.sort_by { |w, c| [-c, w] }.first(3)
raise "top=#{top} exp=#{exp_top}" unless top == exp_top
puts "OK b58_topk_word_freq"
