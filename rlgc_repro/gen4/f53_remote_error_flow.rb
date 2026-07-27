# f53 failing job: worker raises; #value surfaces Ractor::RemoteError with cause chain
# axes: raise-in-ractor, RemoteError.cause, stress bounded around #value (known upstream assert)
Warning[:experimental] = false
Thread.report_on_exception = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

class JobError < StandardError
  attr_reader :job_id
  def initialize(msg, job_id)
    super(msg)
    @job_id = job_id
  end
end

r = Ractor.new do
  jid = Ractor.receive
  begin
    raise ArgumentError, "inner-cause-#{jid}"
  rescue ArgumentError
    raise JobError.new("job #{jid} failed", jid)
  end
end
r.send(77)

# bound stress: #value under active GC.stress can hit known upstream recursive-lock assert
GC.stress = false if STRESS
begin
  r.value
  raise "expected RemoteError"
rescue Ractor::RemoteError => err
  assert err.cause.is_a?(JobError), "cause class #{err.cause.class}"
  assert err.cause.message == "job 77 failed", "cause message"
  assert err.cause.job_id == 77, "cause ivar travels"
  assert err.cause.cause.is_a?(ArgumentError), "nested cause class"
  assert err.cause.cause.message == "inner-cause-77", "nested cause message"
end
GC.stress = true if STRESS
GC.start
puts "OK f53_remote_error_flow"
