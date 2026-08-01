# define_finalizer on a frozen shareable raises FrozenError in any Ractor
# axes: finalizer isolation, frozen shareable, 1 ractor
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
sh = Ractor.make_shareable([1, 2, 3])
raised = false
begin
  ObjectSpace.define_finalizer(sh, proc { })
rescue FrozenError
  raised = true
end
raise 'expected FrozenError on frozen shareable' unless raised
# same rule inside a worker on a shared frozen object
port = Ractor::Port.new
r = Ractor.new(port, sh) do |p, o|
  r2 = false
  begin; ObjectSpace.define_finalizer(o, proc { }); rescue FrozenError; r2 = true; end
  p.send(r2)
  r2
end
raise unless port.receive == true
raise unless r.value == true
GC.compact
puts "OK j32_finalizer_frozen_shareable_error"
