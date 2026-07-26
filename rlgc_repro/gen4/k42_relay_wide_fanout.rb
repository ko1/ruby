# 例外オブジェクトを 3 段の Ractor で中継(各段で copy)、ivar 保持を検証
# axes: relay,copy,fanout
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
class Relay5 < StandardError
  attr_reader :seq, :tag
  def initialize(msg, seq, tag)
    super(msg)
    @seq = seq
    @tag = tag
  end
end
HOPS = 3
outp = Ractor::Port.new
nxt = outp
stages = []
HOPS.times do
  stage = Ractor.new(nxt) do |dst|
    loop do
      msg = Ractor.receive
      break if msg == :stop
      dst.send(msg)
    end
    dst.send(:stop) if dst.is_a?(Ractor)
    :done
  end
  nxt = stage
  stages << stage
end
first = nxt
M = 12
M.times { |x| first.send(Relay5.new("exc-#{x}", x, "t#{x}")) }
got = M.times.map { outp.receive }
GC.compact
first.send(:stop)
stages.each(&:value)
raise "seq" unless got.map(&:seq).sort == (0...M).to_a
raise "class" unless got.all? { |g| g.is_a?(Relay5) }
raise "tag" unless got.map(&:tag).sort == (0...M).map { |x| "t#{x}" }.sort
puts "OK k42_relay_wide_fanout"
