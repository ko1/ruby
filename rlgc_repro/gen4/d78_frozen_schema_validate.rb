# Validation service driven by a frozen shareable schema (nested rules incl.
# lambda checks); accept/reject counts exact. Axes: 90 docs, copy, stress svc.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
RULES = Ractor.make_shareable({
  name: Ractor.shareable_lambda { |v| v.is_a?(String) && !v.empty? },
  age: Ractor.shareable_lambda { |v| v.is_a?(Integer) && v.between?(0, 150) },
  tags: Ractor.shareable_lambda { |v| v.is_a?(Array) && v.all? { _1.is_a?(String) } }
})
done = Ractor::Port.new
svc = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  ok = bad = 0
  loop do
    msg = Ractor.receive
    break if msg == :stop
    doc, rp = msg
    errs = RULES.reject { |field, rule| rule.call(doc[field]) }.keys
    if errs.empty?
      ok += 1
      rp << :valid
    else
      bad += 1
      rp << errs
    end
  end
  GC.stress = false
  done << :done
  [ok, bad]
end
rp = Ractor::Port.new
nok = nbad = 0
90.times do |i|
  doc, want = case i % 5
              when 0, 1 then [{ name: "n#{i}", age: i % 100, tags: ["t"] }, :valid]
              when 2 then [{ name: "", age: 5, tags: [] }, [:name]]
              when 3 then [{ name: "x", age: 999, tags: [1] }, %i[age tags]]
              else [{ age: 10, tags: [] }, [:name]]
              end
  want == :valid ? nok += 1 : nbad += 1
  svc.send([doc, rp])
  raise "doc#{i}" unless rp.receive == want
end
svc.send(:stop)
done.receive
raise unless svc.value == [nok, nbad] && nok == 36 && nbad == 54
puts "OK d78_frozen_schema_validate"
