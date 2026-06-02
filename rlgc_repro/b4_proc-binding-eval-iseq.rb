# RLGC adversarial stress -- EVAL / BINDING / instance_eval / define_method(eval'd proc) angle
# into the cross-objspace eval-produced ISeq/cme lifetime (terminated/orphan-Ractor) face.
#
# Each short-lived worker Ractor builds a fresh class ENTIRELY via eval/binding/define_method,
# so every method body is a per-Ractor ISeq compiled in the WORKER objspace:
#   * class_eval("def ...")            -> m_tbl iseq in worker objspace
#   * define_method(&eval("->{...}"))  -> cme whose body proc/iseq is worker-local
#   * a captured binding eval'ing locals (extra worker-local iseqs/env)
# The worker calls the methods (installs a cc_tbl on the worker-local class), bundles the class
# with a frozen graph + module via Ractor.make_shareable, and sends it to a long-lived HOLDER
# that RETAINS it. The class object is shareable (reachable from main) but its m_tbl/cc_tbl/iseq
# subtree lives in the worker objspace. Workers terminate in waves (objspace orphaned) while a
# hammer Thread + the holder drive global/full GC. A global GC reaches the held class via the
# holder but its eval-built subtree, rooted only through the (orphaned) worker objspace, is
# swept -> "mark T_NONE" / dangling m_tbl in vm_search_cc -> rb_id_table_lookup.
#
# Deterministic: fixed wave/worker/loop counts, no randomness, no wall-clock control flow.

WAVES    = 40
PER_WAVE = 20   # > core count: force concurrent terminations + GCs
INNER    = 24   # classes each worker builds/sends
LOCAL_GC = 6    # worker-local minor GCs

# ---- Holder Ractor: retains shareable eval-built classes from dead workers across waves ----
boot_port = Ractor::Port.new
holder = Ractor.new(boot_port) do |boot|
  port = Ractor::Port.new
  boot.send(port)
  held = []
  total = 0
  loop do
    msg = port.receive
    break if msg == :done
    sh, _un = msg                 # sh = [klass, module, frozen_graph, tag]
    held << sh
    held.shift if held.size > 200 # bounded sliding window: held across + released after term
    total += 1
    if (total & 7) == 0
      junk = Array.new(24) { |i| "holder-#{i}".dup }
      GC.start(full_mark: false)  # local GC of holder while it holds foreign-objspace subtrees
      junk.clear
    end
  end
  total
end
holder_port = boot_port.receive

# Build a shareable bundle whose class's methods are eval/binding/define_method-produced ISeqs.
def build_eval_bundle(tag)
  k = Class.new
  k.class_eval("def compute(x) = x * 2 + #{tag % 97}")           # eval'd literal def -> m_tbl iseq
  k.define_method(:label, &eval("->{ self.class.name.to_s }"))   # define_method(eval'd proc) -> cme
  k.class_eval("def chain(x); compute(x) + #{tag % 31}; end")    # cc_tbl edge compute<-chain
  b = binding; b.eval("anchor#{tag % 4} = #{tag}") rescue nil    # binding eval -> worker-local iseq/env
  inst = k.new
  s = 0
  8.times { |i| s += inst.compute(i) + inst.chain(i) }           # install cc_tbl on worker-local class
  m = Module.new
  frozen_graph = Ractor.make_shareable([tag, s, [tag, tag].freeze, { a: tag, b: s }.freeze])
  Ractor.make_shareable([k, m, frozen_graph, tag])               # deep make_shareable of the bundle
end

def build_unshareable(tag)
  a = []
  6.times do |i|
    a << "u-#{tag}-#{i}".dup
    a << [tag, i, "x#{i}".dup]
  end
  a << { deep: [a.dup, "nested-#{tag}".dup] }
  a
end

# ---- Hammer Thread: continuous full global GC (STW across all Ractors) ----
hammer_done = false
hammer = Thread.new do
  (WAVES * 20).times do
    break if hammer_done
    GC.start                    # full/global STW GC: walks all + orphan objspaces
    GC.start(full_mark: false)
  end
end

waves_ok = 0
WAVES.times do |w|
  workers = PER_WAVE.times.map do |k|
    Ractor.new(holder_port, w, k) do |port, wave, idx|
      tag = wave * 1000 + idx
      sent = 0
      INNER.times do |i|
        sh = build_eval_bundle(tag * 100 + i)
        un = build_unshareable(tag * 100 + i)
        port.send([sh, un])
        sent += 1
        GC.start(full_mark: false) if (i % (INNER / LOCAL_GC)) == 0
      end
      sent
    end
  end
  workers.each(&:join)          # terminate wave -> orphan objspaces holding eval-built subtrees
  waves_ok += 1
end

hammer_done = true
hammer.join
holder_port.send(:done)
produced = holder.value

raise "wave mismatch #{waves_ok}" unless waves_ok == WAVES
raise "holder produced too few #{produced}" unless produced == WAVES * PER_WAVE * INNER
puts "ok"