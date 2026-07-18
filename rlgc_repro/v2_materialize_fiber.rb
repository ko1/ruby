# C2: materialize 中の Fiber.yield (suspend された fiber の EC 上に frame が残る)
Warning[:experimental] = false
class FY
  def initialize; @t = Time.now; end
  def marshal_dump; [1]; end
  def marshal_load(a); Fiber.yield(:mid) if Fiber.current != nil rescue nil; @x = a; end
end
r = Ractor.new do
  40.times do
    f = Fiber.new { Ractor.receive }
    v = f.resume                 # marshal_load の Fiber.yield で :mid が返り、frame は suspend fiber の EC に残る
    3.times { GC.start rescue nil }   # frame が root されているかをここで叩く
    v = f.resume while f.alive?  # 復元を完了させる
  end
  :ok
end
40.times { r.send(FY.new) }
raise "bad" unless r.value == :ok
puts "C2_FIBER_OK"
