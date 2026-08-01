# f17 record shredder pool: 40-ivar objects moved to 2 workers, ivars audited remotely
# axes: move, extended-shape objects, pool, GC.start in worker
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

class Rec40
  def initialize(seed)
    40.times { |i| instance_variable_set(:"@r#{i}", seed + i) }
  end
end

port = Ractor::Port.new
pool = 2.times.map do |wi|
  Ractor.new(port, wi) do |po, myid|
    loop do
      mm = Ractor.receive
      break if mm == :eof
      GC.start
      sum = mm.instance_variables.sum { |nm| mm.instance_variable_get(nm) }
      po.send([myid, mm.instance_variables.size, sum])
    end
  end
end

jobs = STRESS ? 2 : 6
jobs.times do |i|
  seed = i * 100
  rec = Rec40.new(seed)
  pool[i % 2].send(rec, move: true)
  begin
    rec.instance_variable_get(:@r0)
    raise "rec not husked"
  rescue Ractor::MovedError
  end
  wid, cnt, sum = port.receive
  assert wid == i % 2, "routed worker"
  assert cnt == 40, "ivar count #{cnt}"
  assert sum == 40 * seed + (0...40).sum, "ivar sum #{sum}"
end
pool.each { |w| w.send(:eof) }
puts "OK f17_ivar40_move_pool"
