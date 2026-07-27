# f68 result mux: 3 workers each own a moved pipe write-end; main collects results by fd readiness-free reads
# axes: move of IO per worker, pool, results via pipes instead of ports
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

pipes = 3.times.map { IO.pipe }
workers = 3.times.map do |wi|
  r = Ractor.new(wi) do |myid|
    io = Ractor.receive
    job = Ractor.receive
    io.write("w#{myid}:#{job.sum}")
    io.close
    :bye
  end
  r.send(pipes[wi][1], move: true)
  r
end

workers.each_with_index { |r, wi| r.send([wi, wi + 1, wi + 2]) }
GC.start
results = pipes.map { |prd, _| out = prd.read; prd.close; out }
assert results == ["w0:3", "w1:6", "w2:9"], "pipe results #{results.inspect}"
GC.stress = false if STRESS # bound stress around #value (known upstream assert)
assert workers.map(&:value) == [:bye, :bye, :bye], "workers done"
GC.stress = true if STRESS
puts "OK f68_io_pipe_pool_results"
