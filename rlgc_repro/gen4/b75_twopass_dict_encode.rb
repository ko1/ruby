# pass1 で辞書 (word->id) を構築し make_shareable、pass2 の worker 群が corpus を id 列へ encode
# axes: 2+3 workers, shareable dict, copy, decode round-trip 検証
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

WORDS = %w[alpha beta gamma delta eps zeta].freeze
N = 48
corpus = Array.new(N) { |i| WORDS[(i * 5) % 6] }
# pass1: 出現順で id 付け (2 worker が半分ずつ first-seen 順を出し main が合成)
p1 = Ractor::Port.new
w1 = 2.times.map do |wi|
  Ractor.new(p1, corpus[wi * (N / 2), N / 2], wi) do |o, part, id|
    seen = {}
    order = []
    part.each { |w| (seen[w] = true; order << w) unless seen.key?(w) }
    o.send([id, order])
  end
end
orders = Array.new(2)
2.times do
  id, order = p1.receive
  orders[id] = order
end
w1.each(&:value)
dict = {}
(orders[0] + orders[1]).each { |w| dict[w] = dict.size unless dict.key?(w) }
DICT = Ractor.make_shareable(dict.dup)
exp_ids = corpus.map { |w| DICT[w] }
# pass2: 3 worker が slice を encode
p2 = Ractor::Port.new
w2 = 3.times.map do
  Ractor.new(p2, DICT) do |o, d|
    loop do
      msg = Ractor.receive
      break if msg == :stop
      si, sl = msg
      o.send([si, sl.map { |w| d[w] }])
    end
  end
end
slices = corpus.each_slice(8).to_a
slices.each_with_index { |sl, si| w2[si % 3].send([si, sl]) }
got = Array.new(slices.size)
slices.size.times do
  si, ids = p2.receive
  got[si] = ids
end
w2.each { |w| w.send(:stop) }
w2.each(&:value)
raise "encode" unless got.flatten == exp_ids
# decode round-trip
rev = DICT.to_a.to_h { |w, i| [i, w] }
raise "decode" unless got.flatten.map { |i| rev[i] } == corpus
puts "OK b75_twopass_dict_encode"
