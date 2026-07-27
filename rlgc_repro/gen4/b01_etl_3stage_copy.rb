# ETL 3段 pipeline (extract->transform->load) を copy 転送で接続
# axes: chain 3 ractors, copy, GC.compact in transform
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 30
loaded = Ractor::Port.new
load_r = Ractor.new(loaded) do |out|
  total = 0
  cnt = 0
  loop do
    rec = Ractor.receive
    break if rec == :eof
    total += rec[:val]
    cnt += 1
  end
  out.send([cnt, total])
end
xform = Ractor.new(load_r) do |dst|
  i = 0
  loop do
    rec = Ractor.receive
    if rec == :eof
      dst.send(:eof)
      break
    end
    rec[:val] = rec[:raw].bytesize * 3
    dst.send(rec)
    GC.compact if (i += 1) % 15 == 0
  end
end
expected = 0
N.times do |k|
  raw = "record-#{k}-" + ("x" * (k % 17))
  expected += raw.bytesize * 3
  xform.send({ id: k, raw: raw, val: 0 })
end
xform.send(:eof)
cnt, total = loaded.receive
xform.value
load_r.value
raise "cnt=#{cnt}" unless cnt == N
raise "total=#{total} exp=#{expected}" unless total == expected
puts "OK b01_etl_3stage_copy"
