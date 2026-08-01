# Append-only log with 2 tail-reader ractors paging via :read_from(offset);
# both rebuild identical state equal to the writer fold. Axes: 100 records,
# page=16, copy, stress in readers.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
done = Ractor::Port.new
log = Ractor.new(done) do |done|
  recs = []
  loop do
    msg = Ractor.receive
    break if msg == :stop
    op, a, rp = msg
    case op
    when :append then recs << a; rp << recs.size
    when :read_from then rp << recs[a, 16]
    end
  end
  done << :done
  recs.size
end
rp = Ractor::Port.new
rng = Random.new(64)
model = Hash.new(0)
100.times do |i|
  k = "k#{rng.rand(8)}"
  d = rng.rand(1..9)
  model[k] += d
  log.send([:append, [k, d], rp])
  raise unless rp.receive == i + 1
end
readers = 2.times.map do
  Ractor.new(log, done, STRESS) do |log, done, stress|
    GC.stress = true if stress
    my = Ractor::Port.new
    st = Hash.new(0)
    off = 0
    loop do
      log.send([:read_from, off, my])
      page = my.receive
      break if page.nil? || page.empty?
      page.each { |k, d| st[k] += d }
      off += page.size
      break if page.size < 16
    end
    GC.stress = false
    done << :cdone
    [st.dup, off]
  end
end
2.times { raise unless done.receive == :cdone }
states = readers.map(&:value)
raise "readers disagree" unless states[0] == states[1]
raise "fold" unless states[0] == [model, 100]
log.send(:stop)
done.receive
raise unless log.value == 100
puts "OK d64_alog_tail_readers"
