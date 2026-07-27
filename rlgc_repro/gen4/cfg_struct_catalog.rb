# gen4 shared-config: product catalog of frozen Structs (with nested frozen
# arrays) shared by reference; workers build unshareable order books against
# it, die-free, and merge via #value.
# axes: transfer=copy(results), shared frozen Structs, GC=GC.compact once per worker
Product = Struct.new(:sku, :price, :tags)
CATALOG = Ractor.make_shareable(
  Array.new(200) { |i| Product.new("SKU-#{format('%04d', i)}", (i * 7) % 500 + 1, ["t#{i % 6}", "all"]) }
)

N_WORKERS = 5
ORDERS = 250

workers = N_WORKERS.times.map do |wid|
  Ractor.new(wid, ORDERS) do |id, n|
    book = []   # unshareable per-worker state
    revenue = 0
    n.times do |i|
      p_ = CATALOG[(id * 37 + i) % CATALOG.size]
      qty = 1 + (i % 3)
      book << { sku: p_.sku, qty: qty, tags: p_.tags }
      revenue += p_.price * qty
      GC.compact if i == n / 2
      book.shift if book.size > 50
    end
    [revenue, book.size]
  end
end

got_rev = workers.sum { |w| w.value[0] }
exp_rev = 0
N_WORKERS.times do |id|
  ORDERS.times do |i|
    p_ = CATALOG[(id * 37 + i) % CATALOG.size]
    exp_rev += p_.price * (1 + (i % 3))
  end
end
raise "FAIL revenue #{got_rev} != #{exp_rev}" unless got_rev == exp_rev
puts "OK cfg_struct_catalog"
