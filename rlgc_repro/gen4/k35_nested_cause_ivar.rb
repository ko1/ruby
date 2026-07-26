# 内側で raise→外側 rescue が custom 例外で wrap、cause 連鎖を検証
# axes: nested,cause,ivar
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
class Wrap4 < StandardError
  attr_reader :where
  def initialize(msg, where)
    super(msg)
    @where = where
  end
end
N = 12
workers = N.times.map do |k|
  Ractor.new(k) do |id|
    Thread.current.report_on_exception = false
    begin
      begin
        raise ArgumentError, "inner-#{id}"
      rescue
        raise Wrap4.new("outer-#{id}", id)
      end
    rescue Wrap4
      raise
    end
  end
end
fails = 0
workers.each_with_index do |w, k|
  begin
    w.value
    raise "should raise"
  rescue Ractor::RemoteError => rex
    fails += 1
    raise "outer" unless rex.cause.is_a?(Wrap4)
    raise "where" unless rex.cause.where == k
    raise "chain" unless rex.cause.cause.is_a?(ArgumentError)
  end
  GC.compact if k == 5
end
raise "count" unless fails == N
puts "OK k35_nested_cause_ivar"
