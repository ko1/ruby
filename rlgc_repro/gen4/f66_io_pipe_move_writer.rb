# f66 log shipper: IO.pipe write-end MOVED into worker; worker writes lines, main reads to EOF
# axes: move of IO, pipe as result channel, GC.start
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

rd, wr = IO.pipe
w = Ractor.new do
  io = Ractor.receive
  lines = Ractor.receive
  lines.each { |ll| io.write(ll + "\n") }
  io.close
  :done
end
w.send(wr, move: true)
begin
  wr.fileno
  raise "pipe write-end not husked"
rescue Ractor::MovedError, IOError
end

n = STRESS ? 5 : 20
want = n.times.map { |i| "log line #{i} payload=#{i * i}" }
w.send(want)
GC.start
data = rd.read # until worker closes
rd.close
got = data.split("\n")
assert got == want, "pipe content (#{got.size} lines)"
GC.stress = false if STRESS # bound stress around #value (known upstream assert)
assert w.value == :done, "worker exit"
GC.stress = true if STRESS
puts "OK f66_io_pipe_move_writer"
