# C2: 同一 Ractor 複数 thread の receive 交錯 (materialize 中 block で切替を誘発)
Warning[:experimental] = false
class Blk
  def initialize; @t = Time.now; end          # T_DATA で Marshal 経路を強制
  def marshal_dump; [1]; end
  def marshal_load(a); Thread.pass; sleep 0.0005; @x = a; end  # 復元中に切替
end
r = Ractor.new do
  gcth = Thread.new { 120.times { GC.start rescue nil; Thread.pass } }
  ths = 2.times.map { Thread.new { 80.times { Ractor.receive } } }
  ths.each(&:join); gcth.kill; gcth.join
  :ok
end
160.times { r.send(Blk.new) }
raise "bad" unless r.value == :ok
puts "C2_THREADS_OK"
