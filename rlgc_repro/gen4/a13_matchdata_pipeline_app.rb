# ログ解析風: 正規表現マッチ結果を move でパイプライン(courier shareable REF)
# KNOWN-UPSTREAM: RUBY_DEBUG build では upstream の MatchData-move subject 腐敗
# (rb_str_enc_get T_STRING assert, stock f379596fc4 で 6/6)を決定論再現する。
# upstream 修正の canary として残す。soak では KNOWN 扱い。
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
sink = Ractor::Port.new
extractor = Ractor.new(sink) do |out|
  loop do
    m = Ractor.receive
    break if m == :stop
    val = (m[:g] rescue nil)
    out.send(val ? val.to_i : -1)
  end
end
30.times do |k|
  md = "id=#{k*7}!".match(/id=(?<g>\d+)/)
  extractor.send(md, move: true) rescue extractor.send({ g: nil })
  GC.compact if k % 5 == 0
end
extractor.send(:stop)
extractor.value
puts "OK a13"
