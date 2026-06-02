NW = 16
stop = false

hammer = Thread.new do
  until stop
    GC.start(full_mark: true, immediate_sweep: true)
    GC.compact rescue nil
    Thread.pass
  end
end

# main thread B: churn rb_global_tbl structure (insert/alias/trace_var/untrace)
tbl = Thread.new do
  300.times do |i|
    eval("$tg_#{i % 100} = ('payload_' * 4).dup")
    eval("trace_var(:\"$tg_#{i % 100}\") { |v| v.length }") if i % 6 == 0
    eval("untrace_var(:\"$tg_#{i % 100}\") rescue nil") if i % 13 == 0
    eval("alias $ta_#{i % 40} $tg_#{i % 100}") if i % 9 == 0
    GC.start if i % 25 == 0
  end
end

# 3 generations of dying/respawning workers writing shared-slot ractor-local globals
3.times do
  rs = NW.times.map do |n|
    Ractor.new(n) do |id|
      120.times do |i|
        $/  = (+("rsep_#{id}_#{i}_" * 5))                       # -> rb_rs (frozen shareable)
        eval('$-i = (+("isuf_" + id.to_s + "_" + i.to_s))') rescue nil  # -> ARGF.inplace
        $_  = (+("lastline_#{id}_#{i}"))                        # -> per-frame svar
        $DEBUG = (i.even? ? [id, i] : nil)                      # -> r->debug
        $stdout = $stdout                                       # -> r->r_stdout
        _x = $/ ; _y = (eval('$-i') rescue nil) ; _z = $_
        GC.start if i % 15 == 0                                 # owning worker local GC
      end
      :done
    end
  end
  rs.each(&:value)
  GC.start(full_mark: true)
  GC.compact rescue nil
end

tbl.join
stop = true
hammer.join
GC.start(full_mark: true); GC.compact rescue nil
p [$/.length, $_.to_s.length]
puts "OK"
