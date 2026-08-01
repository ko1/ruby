# ivar 持ちオブジェクトを copy 転送する ETL: worker が ivar を読んで集計を ivar に書く
# axes: 3 workers, plain object with ivars, copy round-trip
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

class Parcel
  attr_accessor :id, :body, :w, :score
  def initialize(id, body, w)
    @id = id
    @body = body
    @w = w
    @score = nil
  end
end

N = 20
out = Ractor::Port.new
ws = 3.times.map do
  Ractor.new(out) do |o|
    loop do
      p_ = Ractor.receive
      break if p_ == :stop
      p_.score = p_.body.bytesize * p_.w + p_.id
      o.send(p_)
    end
  end
end
expected = 0
N.times do |k|
  body = "y" * (k % 23 + 1)
  expected += body.bytesize * (k % 5 + 1) + k
  ws[k % 3].send(Parcel.new(k, body, k % 5 + 1))
end
sum = 0
idsum = 0
N.times do
  p_ = out.receive
  sum += p_.score
  idsum += p_.id
end
ws.each { |w| w.send(:stop) }
ws.each(&:value)
raise "sum=#{sum} exp=#{expected}" unless sum == expected
raise "ids" unless idsum == (0...N).sum
puts "OK b08_etl_ivar_payload"
