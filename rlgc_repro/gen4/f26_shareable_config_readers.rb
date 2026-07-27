# f26 config service: make_shareable nested config; N readers verify by reference, no copies
# axes: shareable references, pool, GC.start between reader waves
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

CONF = Ractor.make_shareable(
  { app: "rlgc", limits: { depth: 5, conns: [10, 20, 30] },
    features: [:a, :b, :c], banner: "hello-config" }
)

port = Ractor::Port.new
waves = STRESS ? 1 : 3
waves.times do |wv|
  2.times do |wi|
    Ractor.new(port, CONF, wi, wv) do |po, cc, myid, wave|
      ok = cc[:app] == "rlgc" &&
           cc[:limits][:conns].sum == 60 &&
           cc[:features].include?(:b) &&
           cc[:banner].frozen?
      po.send([wave, myid, ok, cc[:banner].object_id])
    end
  end
  GC.start
  bid = CONF[:banner].object_id
  2.times do
    wave, wid, ok, oid = port.receive
    assert wave == wv, "wave tag"
    assert ok, "reader #{wid} checks (wave #{wv})"
    assert oid == bid, "banner not shared by reference"
  end
end
puts "OK f26_shareable_config_readers"
