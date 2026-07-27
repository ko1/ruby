# f45 matrix cruncher: float-heavy nested arrays moved to worker, row sums verified
# axes: move, Float leaves, husk assert, GC.start
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
    po.send([mm.size, mm.map { |row| row.sum.round(6) }])
  end
end

dim = STRESS ? 4 : 12
mat = dim.times.map { |i| dim.times.map { |j| (i + 1) * 0.5 + j * 0.25 } }
want = mat.map { |row| row.sum.round(6) }
w.send(mat, move: true)
begin
  mat.size
  raise "matrix not husked"
rescue Ractor::MovedError
end
GC.start
rows, sums = port.receive
assert rows == dim, "row count"
assert sums == want, "row sums #{sums.inspect}"
puts "OK f45_float_matrix_move"
