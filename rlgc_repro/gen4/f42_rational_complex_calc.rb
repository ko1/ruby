# f42 math service: Rational and Complex payload leaves, remote arithmetic, exact equality
# axes: copy, Rational/Complex, request/response rounds, GC.start
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
    op, aa, bb = mm
    res = case op
          when :add then aa + bb
          when :mul then aa * bb
          end
    po.send([op, res, res.class.name])
  end
end

jobs = [
  [:add, Rational(1, 3), Rational(1, 6), Rational(1, 2), "Rational"],
  [:mul, Rational(22, 7), Rational(7, 11), Rational(2, 1), "Rational"],
  [:add, Complex(1, 2), Complex(3, -5), Complex(4, -3), "Complex"],
  [:mul, Complex(0, 1), Complex(0, 1), Complex(-1, 0), "Complex"],
]
jobs.each { |op, aa, bb, _, _| w.send([op, aa, bb]) }
GC.start
jobs.each do |op, _, _, want, wantcls|
  gop, res, cls = port.receive
  assert gop == op, "op order"
  assert res == want, "#{op} => #{res}"
  assert cls == wantcls, "class #{cls}"
end
# shareability: Rational/Complex are frozen numerics
assert Ractor.shareable?(Rational(5, 9)) && Ractor.shareable?(Complex(2, 3)), "numerics shareable"
w.send(:eof)
puts "OK f42_rational_complex_calc"
