# f65 ranking service: Comparable objects (custom <=>) sorted remotely, order verified
# axes: copy, Comparable, pool of 2, GC.start
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

class Score
  include Comparable
  attr_reader :name, :pts
  def initialize(name, pts)
    @name = name
    @pts = pts
  end
  def <=>(other)
    pts <=> other.pts
  end
end

port = Ractor::Port.new
pool = 2.times.map do |wi|
  Ractor.new(port, wi) do |po, myid|
    loop do
      mm = Ractor.receive
      break if mm == :eof
      sorted = mm.sort
      po.send([myid, sorted.map(&:name), sorted.first.pts, mm.max.name])
    end
  end
end

groups = [
  [Score.new("c", 30), Score.new("a", 10), Score.new("b", 20)],
  [Score.new("z", 5), Score.new("x", 50), Score.new("y", 25)],
]
groups.each_with_index { |gg, i| pool[i].send(gg) }
GC.start
want = { 0 => [%w[a b c], 10, "c"], 1 => [%w[z y x], 5, "x"] }
2.times do
  wid, names, minpts, maxname = port.receive
  assert names == want[wid][0], "worker #{wid} order #{names.inspect}"
  assert minpts == want[wid][1], "worker #{wid} min"
  assert maxname == want[wid][2], "worker #{wid} max"
end
pool.each { |w| w.send(:eof) }
puts "OK f65_comparable_sort_pool"
