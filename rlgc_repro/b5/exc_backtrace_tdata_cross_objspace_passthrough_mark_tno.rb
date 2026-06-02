# RLGC: exception backtrace T_DATA passed BY REFERENCE cross-objspace (Ractor copy
# passthrough via obj_refer_only_shareables_p), sub-Ractor dies -> orphan objspace
# still owns the live backtrace + its iseqs/strary -> global GC marks the dangling
# edge -> [BUG] try to mark T_NONE in backtrace_mark (vm_backtrace.c:534).
# Tally: 8/8 default, 5/6 tiny-heap, 5/5 RUBY_GC_STRESS=1.
# Run: RUBY_RACTOR_LOCAL_GC=1 /home/ko1/ruby/src/master/ruby this.rb

N_PROD = 24
HOLD = []
HOLD_MUTEX = Mutex.new

hammer = Thread.new do
  loop do
    GC.start(full_mark: true, immediate_sweep: true)
    GC.compact
    Thread.pass
  end
end

reader = Thread.new do
  loop do
    arr = HOLD_MUTEX.synchronize { HOLD.dup }
    arr.each do |e|
      begin
        bl = e.backtrace_locations
        if bl
          bl.each { |loc| loc.path; loc.lineno; loc.label; loc.base_label }
        end
        e.backtrace&.each(&:length)
      rescue => err
      end
    end
    Thread.pass
  end
end

prods = N_PROD.times.map do |i|
  Ractor.new(i) do |id|
    # fresh deep recursive method -> iseqs live in THIS (soon-orphaned) objspace
    src = <<~RUBY
      def deep_#{id}(n)
        return raise("boom \#{n}") if n <= 0
        deep_#{id}(n - 1)
      end
    RUBY
    obj = Object.new
    obj.instance_eval(src)
    excs = []
    50.times do |k|
      begin
        obj.send("deep_#{id}", 40)
      rescue => e
        e.backtrace_locations  # populate rb_backtrace_t w/ iseqs; strary stays nil
        excs << e
      end
      GC.start if k % 7 == 0
    end
    excs  # returned via r.value => COPY: backtrace T_DATA passes through BY REFERENCE
  end
end

prods.each do |r|
  excs = r.value  # sub-Ractor now terminating -> orphaned objspace owns the backtraces
  HOLD_MUTEX.synchronize { HOLD.concat(excs) }
  GC.start
  GC.compact
end

2000.times do
  GC.start(full_mark: true)
  GC.compact if rand < 0.3
end

hammer.kill
reader.kill
puts "DONE ok=#{HOLD.size}"
