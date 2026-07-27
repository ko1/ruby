# Inventory service consuming moved order objects and moving back receipts;
# stock conservation. Axes: 60 orders, move both directions, stress in service.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
class Order
  attr_reader :id, :sku, :qty
  def initialize(id, sku, qty) = (@id = id; @sku = sku; @qty = qty)
end
class Receipt
  attr_reader :oid, :status, :left
  def initialize(oid, status, left) = (@oid = oid; @status = status; @left = left)
end
done = Ractor::Port.new
svc = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  stock = { "sku0" => 40, "sku1" => 40, "sku2" => 40 }
  shipped = Hash.new(0)
  loop do
    msg = Ractor.receive
    break if msg == :stop
    order, rp = msg
    if stock[order.sku] >= order.qty
      stock[order.sku] -= order.qty
      shipped[order.sku] += order.qty
      rp.send(Receipt.new(order.id, :shipped, stock[order.sku]), move: true)
    else
      rp.send(Receipt.new(order.id, :backorder, stock[order.sku]), move: true)
    end
  end
  GC.stress = false
  done << :done
  [stock, shipped.dup]
end
rp = Ractor::Port.new
rng = Random.new(59)
mstock = { "sku0" => 40, "sku1" => 40, "sku2" => 40 }
mship = Hash.new(0)
60.times do |i|
  sku = "sku#{rng.rand(3)}"
  qty = rng.rand(1..6)
  svc.send([Order.new(i, sku.dup, qty), rp], move: true) # dup: keep model's sku string
  rec = rp.receive
  want = if mstock[sku] >= qty
           mstock[sku] -= qty
           mship[sku] += qty
           :shipped
         else
           :backorder
         end
  raise "o#{i}" unless rec.is_a?(Receipt) && rec.oid == i && rec.status == want && rec.left == mstock[sku]
end
svc.send(:stop)
done.receive
stock, shipped = svc.value
raise "model" unless stock == mstock && shipped == mship
%w[sku0 sku1 sku2].each { |s| raise "conserve #{s}" unless stock[s] + shipped[s] == 40 }
puts "OK d59_inv_move_orders"
