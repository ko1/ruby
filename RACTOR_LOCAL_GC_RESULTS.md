# Ractor-local GC — experimental result (shareable)

Experimental per-Ractor local GC for CRuby: each non-main Ractor gets its own
`rb_objspace_t` and collects its own heap **without the stop-the-world VM barrier**,
so Ractors GC in parallel instead of serializing on one global STW GC.
Status: **research prototype**, env-gated (`RUBY_RACTOR_LOCAL_GC=1`), default OFF and
non-regressing. Not upstreamed.

## Environment

| | |
|---|---|
| CPU | AMD Ryzen 9 5900HX (8 cores / 16 threads) |
| OS | Ubuntu 24.04.4 LTS, kernel 6.8.0 |
| Ruby | 4.1.0dev (master `de5545202`) +PRISM, gcc -O3 |

## Benchmark

Fixed total allocation work split across N independent Ractors (no message passing,
no shared mutable state); each Ractor allocates short-lived objects, which drives GC.
Wall-clock only (Process::CLOCK_MONOTONIC). Speedup is vs the same build's 1-Ractor time.

```ruby
def work(iters)
  acc = 0; i = 0
  while i < iters
    a = Array.new(16) { |k| "item-#{k}" }
    h = { x: a, y: a.first(4), z: a.length }
    acc += h[:z]; i += 1
  end
  acc
end
TOTAL = (ENV['TOTAL'] || 8_000_000).to_i
def run(n)
  per = TOTAL / n
  t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  rs = n.times.map { Ractor.new(per) { |p| work(p) } }
  rs.each { |r| r.value }
  Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0
end
```

Run: `./ruby bench.rb` (baseline) vs `RUBY_RACTOR_LOCAL_GC=1 ./ruby bench.rb`.

## Results

Wall-clock seconds (lower is better); speedup = T(1 Ractor) / T(N), same build.

**TOTAL = 8,000,000**

| Ractors | baseline (global STW GC) | Ractor-local GC | wall change |
|--------:|-------------------------:|----------------:|:-----------:|
| 1 | 17.23 s (1.00×) | 17.44 s (1.00×) | +1% |
| 2 | 10.52 s (1.64×) | 10.76 s (1.62×) | +2% |
| 4 | 6.95 s (2.48×) | 6.64 s (2.63×) | −5% |
| 8 | 6.31 s (**2.73×**) | 5.08 s (**3.43×**) | **−20%** |

**TOTAL = 12,000,000** (two trials, stable)

| Ractors | baseline | Ractor-local GC | wall change |
|--------:|---------:|----------------:|:-----------:|
| 1 | 25.8–26.1 s (1.00×) | 26.9–27.0 s (1.00×) | +3% |
| 8 | 9.48–9.58 s (**2.72×**) | 7.49–7.66 s (**3.55×**) | **−21%** |
| 16 | 9.69–9.76 s (**2.67×**) | 7.80–7.81 s (**3.45×**) | **−20%** |

**Takeaway:** parallel per-Ractor GC gives a stable **~20% wall-clock reduction at 8–16
Ractors** (parallel speedup 2.7× → 3.5×) on this allocation-heavy, share-free workload.
At 1 Ractor it is ~3% slower (per-Ractor objspace routing overhead, no parallelism to gain).

## What this measures (and what it doesn't)

- The win comes from removing the stop-the-world barrier: in the baseline, every GC
  pauses all Ractors; here each Ractor collects its own heap concurrently.
- Workload is deliberately the favorable case: independent Ractors, no sends, no shared
  mutable state. Real workloads with sharing/sends are **not** covered yet.

## Prototype limitations (do not over-read the number)

- **No global GC yet:** shareable objects are pinned (never reclaimed) and a terminated
  Ractor's objspace is not freed → memory grows over a long run. Fine for the bounded
  benchmark, not for production.
- `GC.stat` / `GC.total_time` are not per-Ractor-objspace aware (crash) — wall-clock only.
- Incremental marking + lazy sweep are disabled for local objspaces.
- Class-mutation-heavy workloads still have rough edges.
- env-gated, OFF by default; default build verified non-regressing.
