# semi-join: shareable な許可 key 集合 (Hash) で fact を filter し通過数を検証
# axes: 5 workers, shareable set, copy facts
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

ALLOW = Ractor.make_shareable((0...20).select { |k| k % 3 != 0 }.to_h { |k| [k, true] })
N = 45
NW = 5
exp = (0...N).count { |i| ALLOW.key?((i * 7) % 20) }

out = Ractor::Port.new
ws = NW.times.map do
  Ractor.new(out, ALLOW) do |o, allow|
    c = 0
    loop do
      k = Ractor.receive
      break if k == :stop
      c += 1 if allow.key?(k)
    end
    o.send(c)
  end
end
N.times { |i| ws[i % NW].send((i * 7) % 20) }
ws.each { |w| w.send(:stop) }
got = 0
NW.times { got += out.receive }
ws.each(&:value)
raise "got=#{got} exp=#{exp}" unless got == exp
puts "OK b36_join_semi_filter"
