# gen4 shared-config: deep-frozen nested config (hashes/arrays/strings) read
# by all workers on every iteration while unshareable per-worker state churns;
# workers terminate and results merge via #value.
# axes: transfer=none (shared reads), GC=none, exceptions=none, payload=deep config
CONFIG = Ractor.make_shareable(
  {
    tiers: {
      "free"  => { quota: 10,  features: %w[read] },
      "pro"   => { quota: 100, features: %w[read write] },
      "admin" => { quota: 1000, features: %w[read write manage] },
    },
    weights: [3, 1, 4, 1, 5, 9, 2, 6],
    banner: "shared-config-v1",
  }
)

N_WORKERS = 6
ITER = 400

workers = N_WORKERS.times.map do |wid|
  Ractor.new(wid, ITER) do |id, iter|
    tiers = CONFIG[:tiers].keys.sort
    local = Hash.new(0)   # unshareable churn
    scratch = []
    iter.times do |i|
      tier = tiers[(id + i) % tiers.size]
      t = CONFIG[:tiers][tier]
      local[tier] += t[:quota] + t[:features].size
      scratch << CONFIG[:banner].dup + "-#{i}"
      scratch.clear if scratch.size > 30
      local[:w] += CONFIG[:weights][i % CONFIG[:weights].size]
    end
    local
  end
end

merged = Hash.new(0)
workers.each { |w| w.value.each { |k, v| merged[k] += v } }

exp = Hash.new(0)
tiers = CONFIG[:tiers].keys.sort
N_WORKERS.times do |id|
  ITER.times do |i|
    tier = tiers[(id + i) % tiers.size]
    t = CONFIG[:tiers][tier]
    exp[tier] += t[:quota] + t[:features].size
    exp[:w] += CONFIG[:weights][i % CONFIG[:weights].size]
  end
end
raise "FAIL #{merged}" unless merged == exp
puts "OK cfg_lookup_tables"
