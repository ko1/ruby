# f56 postmortem service: 3-level exception cause chain (really raised) copied and walked remotely
# axes: copy, Exception#cause chain, GC.start
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

def build_chain
  begin
    begin
      begin
        raise IOError, "level-0 io"
      rescue IOError
        raise ArgumentError, "level-1 arg"
      end
    rescue ArgumentError
      raise RuntimeError, "level-2 top"
    end
  rescue RuntimeError => err
    err
  end
end

port = Ractor::Port.new
w = Ractor.new(port) do |po|
  mm = Ractor.receive
  chain = []
  cur = mm
  while cur
    chain << [cur.class.name, cur.message]
    cur = cur.cause
  end
  po.send(chain)
end

w.send(build_chain)
GC.start
chain = port.receive
assert chain == [["RuntimeError", "level-2 top"], ["ArgumentError", "level-1 arg"], ["IOError", "level-0 io"]],
       "cause chain #{chain.inspect}"
puts "OK f56_exc_cause_chain_copy"
