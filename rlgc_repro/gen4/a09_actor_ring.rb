# トークンリング: N Ractor が輪で token を move で回す(Ractor#send/receive)
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
N = 8
done = Ractor::Port.new
ractors = []
ractors = N.times.map do |i|
  Ractor.new(i, done) do |id, dport|
    nxt = Ractor.receive  # 隣の Ractor を最初に受け取る
    loop do
      tok = Ractor.receive
      if tok == :stop
        nxt.send(:stop) unless id == N-1
        break
      end
      tok[:hops] += 1
      tok[:path] << id
      if tok[:hops] >= 40
        nxt.send(:stop)
        dport.send(tok[:hops])
        break
      end
      nxt.send(tok, move: true)
    end
    id
  end
end
N.times { |i| ractors[i].send(ractors[(i+1)%N]) }  # 隣を配る
ractors[0].send({ hops: 0, path: [] }, move: true)
h = done.receive
GC.compact
ractors.each(&:value)
raise unless h >= 40
puts "OK a09"
