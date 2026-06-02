S = Struct.new(:a)            # 1 member -> embedded with a spare field slot
st = S.new(0)
st.instance_variable_set(:@g, 1)   # generic ivar stored inline (no table entry)
r = Ractor.new { Ractor.receive }
r.send(st, move: true)             # move_leave -> rb_replace_generic_ivar -> [BUG] unreachable
p :unreachable_should_have_crashed