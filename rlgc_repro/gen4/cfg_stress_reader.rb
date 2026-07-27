# gen4 shared-config: one reader ractor runs GC.stress=true while reading the
# shared frozen table; a second normal reader hammers the same table; tiny.
# axes: transfer=copy(results), GC=GC.stress in 1 ractor, exceptions=none
LOOKUP = Ractor.make_shareable(
  (0...40).to_h { |i| ["key#{i}", { val: i * 3, label: "L#{i}" }] }
)

stressed = Ractor.new do
  GC.stress = true
  sum = 0
  50.times do |i|
    e = LOOKUP["key#{i % 40}"]
    sum += e[:val] + e[:label].size
  end
  GC.stress = false
  sum
end

normal = Ractor.new do
  sum = 0
  2000.times do |i|
    e = LOOKUP["key#{i % 40}"]
    sum += e[:val] + e[:label].size
  end
  sum
end

exp_one = LOOKUP.values.sum { |e| e[:val] + e[:label].size }
s = stressed.value
n = normal.value
raise "FAIL stressed" unless s == 50 / 40 * exp_one + (0...10).sum { |i| LOOKUP["key#{i}"][:val] + LOOKUP["key#{i}"][:label].size }
raise "FAIL normal" unless n == 2000 / 40 * exp_one
puts "OK cfg_stress_reader"
