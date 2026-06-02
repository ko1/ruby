NUSER = 16          # ractors that keep hammering the fstring table concurrently
WAVES = (ENV['WAVES'] || 30).to_i
UNIQ  = (ENV['UNIQ'] || 13000).to_i   # >> 6200 to guarantee at least one resize

# Background users: continuously dedup/insert/look-up fstrings from THEIR objspaces.
users = NUSER.times.map do |uid|
  Ractor.new(uid) do |uid|
    n = 0
    loop do
      m = Ractor.receive
      break if m == :stop
      300.times do |k|
        s = (-("user#{uid}_#{m}_#{k}"))   # insert/lookup in the (maybe-moved) global table
        n += s.bytesize
      end
      n += (-"common_literal_zero").bytesize
      n += (-"common_literal_one").bytesize
    end
    n
  end
end

hammer = Thread.new do
  loop do
    GC.start(full_mark: true)
    GC.compact rescue nil
  end
end

WAVES.times do |w|
  users.each { |u| u.send(w) }   # kick the users to hammer the table concurrently

  # heavy worker: floods the table to force at least one resize (new table object
  # is allocated in THIS worker objspace), then does LOCAL minor GCs which (per the
  # missing-rescue bug) will not mark fstring_table_obj -> may free it.
  heavy = Ractor.new(w, UNIQ) do |w, uniq|
    uniq.times do |i|
      (-("heavy#{w}_uniq_#{i}_#{i & 3}"))
      GC.start(full_mark: false, immediate_sweep: true) if (i % 400) == 0
    end
    GC.start(full_mark: false, immediate_sweep: true)
    GC.start(full_mark: false, immediate_sweep: true)
    :done
  end
  heavy.value
end

users.each { |u| u.send(:stop) }
users.each(&:value)
hammer.kill
hammer.join rescue nil
puts "done"