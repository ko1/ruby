# Event bus fans every event out to two independent folder services; both folds
# must agree with each other and the model. Axes: bus+2 folders, 90 events, copy,
# stress in folders.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
done = Ractor::Port.new
folders = 2.times.map do
  Ractor.new(done, STRESS) do |done, stress|
    GC.stress = true if stress
    st = Hash.new(0)
    cnt = 0
    loop do
      msg = Ractor.receive
      break if msg == :stop
      k, d = msg
      st[k] += d
      cnt += 1
    end
    GC.stress = false
    done << :done
    [st, cnt]
  end
end
bus = Ractor.new(folders, done) do |folders, done|
  n = 0
  loop do
    msg = Ractor.receive
    break if msg == :stop
    ev, rp = msg
    folders.each { |f| f.send(ev) }
    n += 1
    rp << n
  end
  folders.each { |f| f.send(:stop) }
  done << :done
  n
end
rp = Ractor::Port.new
model = Hash.new(0)
rng = Random.new(28)
90.times do |i|
  k = "t#{rng.rand(7)}"
  d = rng.rand(1..4)
  model[k] += d
  bus.send([[k, d], rp])
  raise unless rp.receive == i + 1
end
bus.send(:stop)
3.times { done.receive }
raise unless bus.value == 90
res = folders.map(&:value)
raise "folders disagree" unless res[0] == res[1]
raise "fold" unless res[0] == [model, 90]
puts "OK d28_es_fanout_two_folders"
