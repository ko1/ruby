# gen4 shared-config: hot config reload. Main builds successive shareable
# config generations and pushes them to long-running workers mid-stream;
# workers switch tables between batches; old generations become garbage.
# axes: transfer=copy jobs + shared config gens, GC=GC.start on reload, exceptions=none
N_WORKERS = 4
N_GENS = 5
BATCH = 60

mk_config = lambda do |gen|
  Ractor.make_shareable(
    { gen: gen, factor: gen + 1, table: Array.new(50) { |i| "g#{gen}-#{i}" } }
  )
end

results = Ractor::Port.new
workers = N_WORKERS.times.map do |wid|
  Ractor.new(results, wid) do |res, id|
    conf = Ractor.receive   # initial config
    while (msg = Ractor.receive) != :stop
      if msg.is_a?(Hash) && msg[:gen]
        conf = msg          # hot reload
        GC.start
      else
        i = msg
        entry = conf[:table][i % conf[:table].size]
        res << [id, conf[:gen], i * conf[:factor] + entry.size]
      end
    end
    conf[:gen]
  end
end

configs = N_GENS.times.map { |g| mk_config.call(g) }
workers.each { |w| w << configs[0] }

expected = 0
N_GENS.times do |g|
  if g > 0
    workers.each { |w| w << configs[g] }
    GC.start
  end
  BATCH.times do |b|
    i = g * BATCH + b
    workers[b % N_WORKERS] << i
    entry = configs[g][:table][i % 50]
    expected += i * (g + 1) + entry.size
  end
  # drain the batch before reloading so every job is computed under gen g
  got = 0
  BATCH.times do
    _id, gen, v = results.receive
    raise "FAIL wrong gen #{gen} != #{g}" unless gen == g
    got += v
  end
  expected -= got # running check: batch matched
  raise "FAIL batch g#{g}" unless expected == 0
  expected = 0
end
workers.each { |w| w << :stop }
raise "FAIL final gen" unless workers.map(&:value).uniq == [N_GENS - 1]
puts "OK cfg_hot_reload"
