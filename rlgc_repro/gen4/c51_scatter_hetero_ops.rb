# c51: heterogeneous scatter: each worker applies a different op (:sum/:max/
# :count3/:sqsum) to the same broadcast data; gather validates each op result.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

OPS = [:sum, :max, :count3, :sqsum].freeze
PER = STRESS ? 12 : 50
DATA = (0...PER).map { |i| (i * 19) % 23 }

gather = Ractor::Port.new
ws = OPS.each_with_index.map do |op, i|
  Ractor.new(gather, op, i) do |g, o, wid|
    tag, arr = Ractor.receive
    raise "chunk" unless tag == :data
    v =
      case o
      when :sum    then arr.sum
      when :max    then arr.max
      when :count3 then arr.count { |x| x % 3 == 0 }
      when :sqsum  then arr.sum { |x| x * x }
      else raise "op"
      end
    g << [:result, wid, o, v]
    :op_done
  end
end
ws.each { |w| w.send([:data, DATA]) }

expect = {
  sum: DATA.sum, max: DATA.max,
  count3: DATA.count { |x| x % 3 == 0 }, sqsum: DATA.sum { |x| x * x },
}
OPS.size.times do
  tag, _wid, op, v = gather.receive
  raise "result" unless tag == :result
  raise "op #{op}: #{v}" unless v == expect[op]
end
GC.stress = false
ws.each { |r| raise unless r.value == :op_done }
puts "OK c51_scatter_hetero_ops"
