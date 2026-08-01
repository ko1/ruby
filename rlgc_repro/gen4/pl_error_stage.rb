# gen4 pipeline: parse stage receives some malformed lines, rescues, and
# forwards error markers downstream; sink tallies ok vs error separately.
# axes: transfer=copy, GC=none, exceptions=raised+rescued in a stage, payload=strings
N_ITEMS = 420

out = Ractor::Port.new

sink = Ractor.new(out) do |o|
  ok = err = sum = 0
  while (m = Ractor.receive) != :eos
    if m[:err]
      err += 1
    else
      ok += 1
      sum += m[:val]
    end
  end
  o << [ok, err, sum]
end

parse = Ractor.new(sink) do |nxt|
  while (line = Ractor.receive) != :eos
    begin
      k, v = line.split("=")
      nxt << { key: k, val: Integer(v) }
    rescue ArgumentError, TypeError
      nxt << { err: true, raw: line }
    end
  end
  nxt << :eos
end

exp_ok = exp_err = exp_sum = 0
N_ITEMS.times do |i|
  if i % 6 == 4
    exp_err += 1
    parse << "corrupt line #{i}"
  else
    exp_ok += 1
    exp_sum += i * 3
    parse << "key#{i}=#{i * 3}"
  end
end
parse << :eos

ok, err, sum = out.receive
[parse, sink].each(&:join)
raise "FAIL ok" unless ok == exp_ok
raise "FAIL err" unless err == exp_err
raise "FAIL sum" unless sum == exp_sum
puts "OK pl_error_stage"
