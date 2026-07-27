# f74 metrics aggregator: 4 producers fan in partial hashes to one port; main folds and audits
# axes: copy, fan-in, hash merge, GC.start mid-collection
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

port = Ractor::Port.new
nprod = 4
per = STRESS ? 3 : 10
nprod.times do |wi|
  Ractor.new(port, wi, per) do |po, myid, cnt|
    part = Hash.new
    cnt.times { |j| part[:"m#{(myid + j) % 4}"] = (part[:"m#{(myid + j) % 4}"] || 0) + j + 1 }
    po.send([myid, part])
  end
end

merged = Hash.new(0)
got = []
nprod.times do |k|
  wid, part = port.receive
  got << wid
  part.each { |kk, vv| merged[kk] += vv }
  GC.start if k == 1
end
assert got.sort == [0, 1, 2, 3], "all producers reported"
assert merged.values.sum == nprod * (1..per).sum, "grand total #{merged.values.sum}"
assert merged.keys.sort == [:m0, :m1, :m2, :m3], "metric keys"
puts "OK f74_fanin_aggregator"
