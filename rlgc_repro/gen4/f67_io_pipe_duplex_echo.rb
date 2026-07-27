# f67 echo service: two pipes; worker gets (req-read, res-write) via move, echoes upcased
# axes: move of both IO endpoints, duplex request/response over pipes
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

req_rd, req_wr = IO.pipe
res_rd, res_wr = IO.pipe
w = Ractor.new do
  rio = Ractor.receive
  wio = Ractor.receive
  data = rio.read # to EOF
  rio.close
  wio.write(data.upcase)
  wio.close
  data.bytesize
end
w.send(req_rd, move: true)
w.send(res_wr, move: true)

msg = "hello duplex pipes " * (STRESS ? 3 : 20)
req_wr.write(msg)
req_wr.close
GC.start
echoed = res_rd.read
res_rd.close
assert echoed == msg.upcase, "echo content"
assert echoed.bytesize == msg.bytesize, "echo size"
GC.stress = false if STRESS # bound stress around #value (known upstream assert)
assert w.value == msg.bytesize, "worker byte count"
GC.stress = true if STRESS
puts "OK f67_io_pipe_duplex_echo"
