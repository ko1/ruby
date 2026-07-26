# ivar 付き custom 例外を port 経由で送る(copy)→ ivar を検証
# axes: ivar,custom,copy
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
class MyErr4 < StandardError
  attr_reader :code, :data
  def initialize(msg, code, data)
    super(msg)
    @code = code
    @data = data
  end
end
N = 12
port = Ractor::Port.new
workers = N.times.map do |k|
  Ractor.new(port, k) do |pt, id|
    Thread.current.report_on_exception = false
    begin
      raise MyErr4.new("err-#{id}", id, [1,2,3].freeze)
    rescue MyErr4 => ex
      pt.send(ex)
    end
    :sent
  end
end
workers.each(&:value)
got = N.times.map { port.receive }
GC.compact
codes = got.map(&:code).sort
raise "codes" unless codes == (0...N).to_a
raise "data" unless got.all? { |g| g.data == [1,2,3].freeze }
raise "msg" unless got.map(&:message).sort == (0...N).map { |x| "err-#{x}" }.sort
puts "OK k11_ivar_frozen_payload"
