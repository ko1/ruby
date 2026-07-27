# f73 assembly line: 3-stage move pipeline; each stage mutates a mixed-type workpiece in place
# axes: move end-to-end, mixed payload (hash+struct+string), GC.start per stage
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

Part = Struct.new(:name, :mass)

out = Ractor::Port.new
paint = Ractor.new(out) do |po|
  loop do
    mm = Ractor.receive
    break if mm == :eof
    mm[:log] << :painted
    mm[:body] << "-painted"
    po.send(mm, move: true)
  end
end
weld = Ractor.new(paint) do |nxt|
  loop do
    mm = Ractor.receive
    if mm == :eof
      nxt.send(:eof)
      break
    end
    GC.start
    mm[:log] << :welded
    mm[:parts] << Part.new("seam", 1)
    nxt.send(mm, move: true)
  end
end
cut = Ractor.new(weld) do |nxt|
  loop do
    mm = Ractor.receive
    if mm == :eof
      nxt.send(:eof)
      break
    end
    mm[:log] << :cut
    nxt.send(mm, move: true)
  end
end

rounds = STRESS ? 2 : 4
rounds.times do |i|
  piece = { id: i, body: +"chassis#{i}", parts: [Part.new("frame", 10)], log: [] }
  cut.send(piece, move: true)
  begin
    piece[:id]
    raise "piece not husked"
  rescue Ractor::MovedError
  end
  done = out.receive
  assert done[:id] == i, "id"
  assert done[:log] == [:cut, :welded, :painted], "stage order #{done[:log].inspect}"
  assert done[:body] == "chassis#{i}-painted", "body"
  assert done[:parts].map(&:name) == %w[frame seam], "parts"
  assert done[:parts].sum(&:mass) == 11, "mass"
end
cut.send(:eof)
puts "OK f73_pipeline3_move_mixed"
