# f69 dedup accumulator: worker keeps custom-key set across rounds; equal keys from later sends collapse
# axes: copy, custom #hash/#eql? equality across independent copies, stateful worker
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

class Fp
  attr_reader :algo, :digest
  def initialize(algo, digest)
    @algo = algo
    @digest = digest
  end
  def hash
    [algo, digest].hash
  end
  def eql?(other)
    other.is_a?(Fp) && other.algo == algo && other.digest == digest
  end
end

port = Ractor::Port.new
w = Ractor.new(port) do |po|
  seen = {}
  loop do
    mm = Ractor.receive
    break if mm == :eof
    fresh = 0
    mm.each do |kk|
      unless seen.key?(kk)
        seen[kk] = true
        fresh += 1
      end
    end
    po.send([fresh, seen.size])
  end
end

rounds = STRESS ? 2 : 4
total_uniqs = 0
rounds.times do |i|
  batch = [Fp.new(:sha, "d#{i}"), Fp.new(:sha, "d0"), Fp.new(:md5, "d#{i}")]
  w.send(batch)
  fresh, size = port.receive
  # every round: sha/d_i + md5/d_i are new; sha/d0 collapses (in-batch dup on round 0)
  want_fresh = 2
  total_uniqs += want_fresh
  assert fresh == want_fresh, "round #{i}: fresh #{fresh}"
  assert size == total_uniqs, "round #{i}: cumulative #{size}"
end
w.send(:eof)
puts "OK f69_custom_key_dedup"
