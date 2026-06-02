N = (ENV['N'] || 24).to_i
ITER = (ENV['ITER'] || 500).to_i

hammer = Thread.new do
  loop do
    GC.start(full_mark: true, immediate_sweep: true)
    GC.compact rescue nil
  end
end
hammer.report_on_exception = false

rs = (1..N).map do |id|
  Ractor.new(id, ITER) do |id, iter|
    iter.times do |i|
      # Fresh module including Kernel -> new Kernel iclass in this worker objspace,
      # so calling Integer()/Float() through it creates a COMPLEMENTED iseq_overload
      # cme (key) + overloaded value (me) physically in THIS worker's objspace, keyed
      # into the VM-global overloaded_cme_table (st_insert under the VM lock).
      m = Module.new { include Kernel }
      k = Class.new
      k.include(m)
      obj = k.new
      obj.instance_eval do
        Integer("7")
        Float("2.5")
      end
      sub = Class.new(Array) { include(Module.new { include Comparable }) }
      sub.new(3) { |j| j }.first
      sub.new(2) { |j| j }.last
      # Drop everything; the complemented iseq_overload cmes are now garbage. The
      # worker's lock-free local GC would sweep them (and the table value) if they were
      # not FL_SHAREABLE -- but they are pinned, so no UAF / no unlocked st_delete race.
      m = k = obj = sub = nil
      GC.start
    end
    :ok
  end
end

rs.each { |r| (r.respond_to?(:take) ? r.take : r.value) rescue nil }
puts "done"