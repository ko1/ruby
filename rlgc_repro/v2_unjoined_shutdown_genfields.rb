Warning[:experimental] = false
rs = 6.times.map do
  Ractor.new do
    # generic ivar host (非 T_OBJECT に ivar)
    a = []
    100.times { |i| a << Array.new(3).tap { |o| o.instance_variable_set(:@g, "v#{i}") } }
    Ractor.receive  # join されず親の exit まで生存 → 親 exit 直前に kill される
  end
end
sleep 0.1
:ok  # main は join せず exit → terminate_all → absorb_all_zombies
