# 例外オブジェクトを通常ペイロードとして send/copy し worker で加工
# axes: payload,copy,compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
class Note4 < StandardError
  attr_reader :level
  def initialize(msg, level)
    super(msg)
    @level = level
  end
end
N = 12
outp = Ractor::Port.new
NW = 4
workers = NW.times.map do |wid|
  Ractor.new(outp) do |op|
    loop do
      e = Ractor.receive
      break if e == :stop
      op.send([e.level, e.message.upcase])
    end
    :done
  end
end
N.times { |x| workers[x % NW].send(Note4.new("note-#{x}", x % 5)) }
got = N.times.map { outp.receive }
GC.compact
workers.each { |w| w.send(:stop) }
workers.each(&:value)
raise "levels" unless got.map(&:first).sort == (0...N).map { |x| x % 5 }.sort
raise "msgs" unless got.map(&:last).all? { |m| m.start_with?("NOTE-") }
puts "OK k71_exc_as_payload_compact"
