# worker pool で map-reduce: main が job を配り worker が結果を Port へ返す
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
NW = 6
reply = Ractor::Port.new
workers = NW.times.map do |wid|
  Ractor.new(reply, wid) do |out, id|
    loop do
      job = Ractor.receive
      break if job == :stop
      n, data = job
      acc = data.sum { |s| s.bytesize }
      out.send([n, acc])
    end
    :done
  end
end
jobs = 200
jobs.times { |n| workers[n % NW].send([n, Array.new(20) { +"item-#{n}-#{_1}" }]) }
got = {}
jobs.times { n, acc = reply.receive; got[n] = acc }
GC.compact
workers.each { |w| w.send(:stop) }
workers.each(&:value)
raise "lost" unless got.size == jobs
puts "OK a01"
