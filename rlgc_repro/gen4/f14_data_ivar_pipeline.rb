# f14 enrichment pipeline: Data subclass with ivar set in initialize, copy through 2 stages
# axes: copy, Data+ivar, chain lifecycle
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

Base = Data.define(:name, :score)
class Enriched < Base
  attr_reader :grade
  def initialize(name:, score:)
    @grade = score >= 50 ? :pass : :fail
    super(name: name, score: score)
  end
end

out = Ractor::Port.new
s2 = Ractor.new(out) do |po|
  loop do
    mm = Ractor.receive
    break if mm == :eof
    po.send([mm.name, mm.score, mm.grade, mm.frozen?])
  end
end
s1 = Ractor.new(s2) do |nxt|
  loop do
    mm = Ractor.receive
    if mm == :eof
      nxt.send(:eof)
      break
    end
    nxt.send(mm) # copy onward
  end
end

recs = [["ann", 72], ["bob", 31], ["cyd", 50]]
recs.each { |nm, sc| s1.send(Enriched.new(name: nm, score: sc)) }
GC.start
recs.each do |nm, sc|
  gname, gscore, ggrade, gfz = out.receive
  assert gname == nm && gscore == sc, "fields for #{nm}"
  assert ggrade == (sc >= 50 ? :pass : :fail), "ivar grade for #{nm}"
  assert gfz, "Data subclass stays frozen"
end
s1.send(:eof)
puts "OK f14_data_ivar_pipeline"
