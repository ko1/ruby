# fan-out/fan-in: 大きな hash を分割 move、結果を merge
# KNOWN-UPSTREAM: GC.stress 下で Ractor#join(monitor)が ractor lock 保持中に alloc→GC
# → vm_lock_enter の locked_by assert(stock f379596fc4 で 6/6)。gcrace_045 と同族。
# upstream 修正の canary として残す。soak では KNOWN 扱い。
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
10.times do |round|
  reply = Ractor::Port.new
  ws = 5.times.map do |wid|
    Ractor.new(reply) do |out|
      part = Ractor.receive
      counts = Hash.new(0)
      part.each { |w| counts[w[0]] += 1 }
      out.send(counts)
    end
  end
  words = Array.new(250) { +("abcde"[rand(5)]) * (rand(3)+1) }
  words.each_slice(50).with_index { |sl, i| ws[i].send(sl, move: true) }
  merged = Hash.new(0)
  5.times { reply.receive.each { |k, v| merged[k] += v } }
  ws.each(&:value)
  raise unless merged.values.sum == 250
  GC.compact
end
puts "OK a07"
