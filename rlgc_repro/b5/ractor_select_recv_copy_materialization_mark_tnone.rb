# RLGC-specific [BUG] try to mark T_NONE in the Ractor.select / port receive path.
# A default Ractor#send of an unshareable deep graph is a COPY message. Under RLGC the RECEIVER
# re-materializes the copy into its own objspace (ractor_basket_accept -> ractor_copy ->
# obj_traverse_replace_i -> #clone). Each #clone allocates = a GC safepoint; mid-traversal the
# half-built clone array references an out-of-heap/freed child, and the GC triggered by the next
# allocation walks that dangling slot -> "try to mark T_NONE".
#
# Run: RUBY_RACTOR_LOCAL_GC=1 RUBY_GC_STRESS=1 /home/ko1/ruby/src/master/ruby this.rb
# Result: 5/5 [BUG] try to mark T_NONE (exit 134). RLGC OFF: 0 crashes. GC.compact NOT required.
# (A heavier 24-port Ractor.select + GC.compact-hammer variant crashes ~3/4 without GC_STRESS.)

def g(d, w)
  d <= 0 ? [:l, "x" * 16, Object.new, { p: "q".dup }] :
    (a = Array.new(w) { g(d - 1, w) }; s = "s#{d}".dup; [a, s, { a: a, s: s }, a])
end

port = Ractor::Port.new
s = Ractor.new(port) { |port| 2000.times { port.send(g(3, 3)) rescue nil }; port.send(:done) }

loop do
  rpv, val = Ractor.select(port)   # receive-side re-materialization races allocation-triggered GC
  break if val == :done
end

s.value rescue nil
puts "survived"