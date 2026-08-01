# quote 付き CSV の簡易パーサを worker に持たせ、field 数と quote 内 comma 数を検証
# axes: 2 workers, copy, custom parse loop
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

def naive_csv(line)
  fields = []
  cur = +""
  inq = false
  line.each_char do |ch|
    if ch == '"'
      inq = !inq
    elsif ch == "," && !inq
      fields << cur
      cur = +""
    else
      cur << ch
    end
  end
  fields << cur
end

N = 24
lines = Array.new(N) { |i| "a#{i},\"x,y#{i}\",tail#{i % 3}" }
out = Ractor::Port.new
ws = 2.times.map do
  Ractor.new(out) do |o|
    loop do
      l = Ractor.receive
      break if l == :stop
      f = naive_csv(l)
      o.send([f.size, f[1].count(",")])
    end
  end
end
N.times { |i| ws[i % 2].send(lines[i]) }
nf = nc = 0
N.times do
  a, b = out.receive
  nf += a
  nc += b
end
ws.each { |w| w.send(:stop) }
ws.each(&:value)
raise "fields=#{nf}" unless nf == N * 3 # quote 内 comma は区切りでない
raise "commas=#{nc}" unless nc == N # 各行 1 個の quote 内 comma
puts "OK b28_csv_quoted_fields"
