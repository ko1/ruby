# f31 text transformer: UTF-8 multibyte strings, char-level transforms, encoding asserts
# axes: copy, UTF-8, worker transform, GC.start
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

port = Ractor::Port.new
w = Ractor.new(port) do |po|
  loop do
    mm = Ractor.receive
    break if mm == :eof
    rev = mm.chars.reverse.join
    po.send([rev, rev.encoding.name, mm.length, mm.bytesize, mm.valid_encoding?])
  end
end

samples = ["こんにちは", "naïve café", "Ω≈ç√∫", "日本語テキスト処理"]
samples.each do |s|
  w.send(s)
  rev, enc, len, bs, valid = port.receive
  assert enc == "UTF-8", "encoding #{enc}"
  assert valid, "valid encoding"
  assert len == s.length && bs == s.bytesize, "length/bytesize"
  assert bs > len, "multibyte sample must have bytesize > length (#{s})"
  assert rev == s.chars.reverse.join, "reverse round-trip"
  assert rev.chars.reverse.join == s, "double reverse identity"
end
GC.start
w.send(:eof)
puts "OK f31_utf8_transformer"
