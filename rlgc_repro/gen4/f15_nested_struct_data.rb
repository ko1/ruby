# f15 order processor: Struct-of-Data-of-Struct nesting, copy and move rounds
# axes: copy+move alternating, nested Struct/Data, GC.start
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

Line = Struct.new(:sku, :qty)
Address = Data.define(:city, :zip)
Order = Struct.new(:id, :ship_to, :lines)

def mk_order(i)
  Order.new(i, Address.new(city: "city#{i}", zip: 10_000 + i),
            [Line.new("sku-a", i + 1), Line.new("sku-b", 2 * i + 1)])
end

port = Ractor::Port.new
w = Ractor.new(port) do |po|
  loop do
    mm = Ractor.receive
    break if mm == :eof
    total = mm.lines.sum(&:qty)
    po.send([mm.id, mm.ship_to.city, mm.ship_to.zip, total])
  end
end

rounds = STRESS ? 2 : 6
rounds.times do |i|
  o = mk_order(i)
  if i.odd?
    w.send(o, move: true)
    begin
      o.id
      raise "order not husked"
    rescue Ractor::MovedError
    end
  else
    w.send(o)
    assert o.ship_to.city == "city#{i}", "copy source intact"
  end
  oid, city, zip, total = port.receive
  assert oid == i && city == "city#{i}" && zip == 10_000 + i, "round #{i} fields"
  assert total == (i + 1) + (2 * i + 1), "round #{i} qty total"
  GC.start if i == 1
end
w.send(:eof)
puts "OK f15_nested_struct_data"
