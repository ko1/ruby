# 多段 cause 連鎖(4段)を worker で作り .value 経由で検証
# axes: cause,chain,mixed
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
N = 12
DEPTH = 4
workers = N.times.map do |k|
  Ractor.new(k) do |id|
    Thread.current.report_on_exception = false
    build = lambda do |n|
      begin
        if n <= 1
          raise "root-#{id}"
        else
          build.call(n - 1)
        end
      rescue
        raise RuntimeError, "level-#{n}-#{id}"
      end
    end
    build.call(DEPTH)
  end
end
fails = 0
workers.each_with_index do |w, k|
  begin
    w.value
  rescue Ractor::RemoteError => rex
    fails += 1
    depth = 0
    cur = rex.cause
    while cur
      depth += 1
      cur = cur.cause
    end
    raise "depth #{depth}" unless depth == DEPTH + 1
  end
  GC.compact if k == 6
end
raise "count" unless fails == N
puts "OK k77_cause_chain_mixed_types"
