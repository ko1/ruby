# 例外 copy の最中に compaction を強く回す
# axes: compact,copy,double
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
class Big2 < StandardError
  attr_reader :blob, :n
  def initialize(msg, blob, n)
    super(msg)
    @blob = blob
    @n = n
  end
end
N = 12
port = Ractor::Port.new
workers = N.times.map do |k|
  Ractor.new(port, k) do |pt, id|
    Thread.current.report_on_exception = false
    blob = Array.new(12) { +"blob-#{id}-#{_1}" }
    begin
      raise Big2.new("big-#{id}", blob, id)
    rescue Big2 => ex
      pt.send(ex)
    end
    :done
  end
end
workers.each(&:value)
got = N.times.map { port.receive }
GC.compact; GC.compact
raise "n" unless got.map(&:n).sort == (0...N).to_a
raise "blob" unless got.all? { |g| g.blob.length == 12 }
puts "OK k80_compact_double_pass"
