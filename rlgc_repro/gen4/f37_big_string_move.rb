# f37 bulk shipper: 64KB strings moved to checksummer; source husked; result via port
# axes: move, large heap-allocated strings, GC.start between rounds
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

port = Ractor::Port.new
w = Ractor.new(port) do |po|
  loop do
    mm = Ractor.receive
    break if mm == :eof
    sum = 0
    mm.each_byte { |bb| sum = (sum + bb) & 0xffff }
    po.send([mm.bytesize, sum, mm[0, 8], mm[-8, 8]])
  end
end

rounds = STRESS ? 2 : 3
rounds.times do |i|
  unit = "chunk#{i}-"
  big = unit * (65_536 / unit.bytesize)
  want_bs = big.bytesize
  want_sum = 0
  big.each_byte { |bb| want_sum = (want_sum + bb) & 0xffff }
  head = big[0, 8]
  tail = big[-8, 8]
  w.send(big, move: true)
  begin
    big.bytesize
    raise "big string not husked"
  rescue Ractor::MovedError
  end
  bs, sum, ghead, gtail = port.receive
  assert bs == want_bs, "bytesize #{bs}"
  assert sum == want_sum, "checksum"
  assert ghead == head && gtail == tail, "head/tail content"
  GC.start
end
w.send(:eof)
puts "OK f37_big_string_move"
