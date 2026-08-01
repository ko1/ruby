# frozen string に generic ivar を付け make_shareable、cross-Ractor read
# axes: strings=100 readers=3 compacts=6 generic_ivar
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
CNT = 100
arr = Array.new(CNT) do |i|
  s = +("s" * ((i % 5) + 1))
  s.instance_variable_set(:@weight, i + 1)
  s.instance_variable_set(:@kind, (i % 3).freeze)
  s.freeze
end
STRS = Ractor.make_shareable(arr.freeze)
EXP = (0...CNT).sum { |i| (i + 1) * ((i % 5) + 1) + (i % 3) }
rs = 3.times.map do |rid|
  Ractor.new(STRS, rid) do |strs, id|
    acc = 0
    strs.each do |s|
      acc += s.instance_variable_get(:@weight) * s.bytesize + s.instance_variable_get(:@kind)
    end
    acc
  end
end
6.times { GC.compact }
rs.each { |ra| raise "mismatch" unless ra.value == EXP }

puts "OK i25_frozen_str_ivar"
