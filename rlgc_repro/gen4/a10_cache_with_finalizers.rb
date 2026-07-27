# 各 Ractor が自オブジェクトに finalizer を張りつつ churn(所有権ベース finalizer)
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
ws = 5.times.map do |i|
  Ractor.new(i) do |id|
    fin = 0
    100.times do |k|
      o = Object.new
      ObjectSpace.define_finalizer(o, proc { }) rescue nil
      o.instance_variable_set(:@k, k)
      GC.start if k % 20 == 0
    end
    :ok
  end
end
GC.compact
raise unless ws.map(&:value).all? { |v| v == :ok }
puts "OK a10"
