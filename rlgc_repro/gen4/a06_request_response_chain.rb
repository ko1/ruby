# request/response を Ractor#value 連鎖で(value of value of value)
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
15.times do
  db = Ractor.new { { rows: Array.new(10) { |i| { id: i, name: +"n#{i}" } } } }
  svc = Ractor.new(db) { |d| data = d.value; { count: data[:rows].size, first: data[:rows].first[:name] } }
  api = Ractor.new(svc) { |s| res = s.value; "count=#{res[:count]} first=#{res[:first]}" }
  raise unless api.value == "count=10 first=n0"
  GC.compact
end
puts "OK a06"
