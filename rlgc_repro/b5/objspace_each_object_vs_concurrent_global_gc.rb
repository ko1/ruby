# NEGATIVE RESULT (no crash after ~35+ runs across default / tiny-heap / GC_STRESS).
# Strongest scenario tried: force the each_object live-list CURSOR page to be
# emptied AND freed mid-walk by a concurrent global STW GC, hoping the resume
# ccan_list_next(cursor) derefs freed memory. Robust because the walker's C frame
# (VALUE v on the machine stack) conservatively pins the cursor page during the
# STW sweep, so it never goes empty/freed.
WALKERS = (ENV['WALKERS'] || 16).to_i
SECS    = (ENV['SECS']    || 16).to_i

walkers = WALKERS.times.map do |wid|
  Ractor.new(wid, SECS) do |wid, secs|
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + secs
    while Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
      keep = Array.new(20_000) { |k| (k & 1 == 0) ? Object.new : "g#{k}".dup }
      n = 0
      ObjectSpace.each_object do |o|
        n += 1
        keep = nil if n == 1500          # kill the whole heap mid-walk
        x = [o] if (n & 7) == 0          # allocate => barrier safepoint
      end
    end
    :ok
  end
end

stop = false
hammers = 8.times.map do
  Thread.new do
    until stop
      # global STW full GC with immediate sweep -> frees the walkers' empty pages
      GC.start(full_mark: true, immediate_mark: true, immediate_sweep: true)
    end
  end
end

walkers.each(&:value)   # NOTE: this build's Ractor API uses .value, not .take
stop = true
hammers.each(&:join)
puts "done"
# Run: RUBY_RACTOR_LOCAL_GC=1 RUBY_GC_HEAP_INIT_SLOTS=2000 ruby this.rb