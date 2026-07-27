# c43: all-of combinator: N futures resolve onto one gather port in arbitrary
# order; main asserts the complete result set. Struct payloads.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

N = STRESS ? 4 : 8
Res = Struct.new(:id, :val)

gather = Ractor::Port.new
prods = N.times.map do |i|
  Ractor.new(gather, i) do |g, id|
    acc = 0
    (id * 50 + 10).times { |k| acc += k }   # varied work to shuffle finish order
    g << Res.new(id, acc)
    :produced
  end
end

got = {}
N.times do
  r = got_r = gather.receive
  raise "type" unless r.is_a?(Res)
  raise "dup" if got.key?(r.id)
  got[r.id] = r.val
end
N.times do |i|
  expect = (0...(i * 50 + 10)).sum
  raise "val #{i}" unless got[i] == expect
end
GC.stress = false
prods.each { |r| raise unless r.value == :produced }
puts "OK c43_future_all_of"
