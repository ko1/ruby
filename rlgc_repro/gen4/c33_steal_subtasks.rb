# c33: work stealing where tasks spawn subtasks (binary tree to depth D);
# outstanding-count termination; processed count == n*(2^(D+1)-1).
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false

W = STRESS ? 3 : 4
ROOTS = 2
D = STRESS ? 2 : 3

coord = Ractor.new(W, ROOTS, D) do |w, roots, d|
  q = Array.new(roots) { |i| [d, i] }
  pending = []
  outstanding = 0
  processed = 0
  dones = 0
  while dones < w
    msg = Ractor.receive
    case msg[0]
    when :get
      if (task = q.shift)
        outstanding += 1
        processed += 1
        msg[1] << [:task, task]
      elsif outstanding == 0
        dones += 1
        msg[1] << [:done]
      else
        pending << msg[1]
      end
    when :put
      q << msg[1]
      if (p = pending.shift)
        task = q.shift
        outstanding += 1
        processed += 1
        p << [:task, task]
      end
    when :task_done
      outstanding -= 1
      raise "neg outstanding" if outstanding < 0
      if outstanding == 0 && q.empty?
        pending.each { |p| p << [:done] }
        dones += pending.size
        pending = []
      end
    end
  end
  raise "left" unless q.empty? && pending.empty? && outstanding == 0
  processed
end

res = Ractor::Port.new
ws = W.times.map do |i|
  Ractor.new(coord, res, i) do |c, rp, wid|
    GC.stress = true if ENV['S_STRESS']
    my = Ractor::Port.new
    cnt = 0
    loop do
      c.send([:get, my])
      msg = my.receive
      break if msg[0] == :done
      depth, val = msg[1]
      cnt += 1
      if depth > 0
        c.send([:put, [depth - 1, val * 2]])
        c.send([:put, [depth - 1, val * 2 + 1]])
      end
      c.send([:task_done])
    end
    GC.stress = false
    rp << [:res, wid, cnt]
  end
end

cnt = 0
W.times do
  tag, _, c = res.receive
  raise "res" unless tag == :res
  cnt += c
end
expected = ROOTS * (2**(D + 1) - 1)
raise "cnt #{cnt} != #{expected}" unless cnt == expected
raise unless coord.value == expected
ws.each(&:value)
GC.start
puts "OK c33_steal_subtasks"
