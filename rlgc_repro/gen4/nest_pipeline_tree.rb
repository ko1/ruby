# gen4 nested: each of 3 supervisors runs an INTERNAL 2-stage pipeline
# (parse -> score, both sub-ractors) over its own item stream; main scatters
# items to supervisors and gathers scored results from one shared port.
# axes: transfer=copy, GC=GC.start in stage2, exceptions=none, depth=2 + pipeline
N_SUP = 3
ITEMS_PER_SUP = 80

out = Ractor::Port.new
sups = N_SUP.times.map do |sid|
  Ractor.new(out, sid, ITEMS_PER_SUP) do |o, sup_id, n|
    score = Ractor.new(o, sup_id) do |oo, s|
      done = 0
      while (m = Ractor.receive) != :eos
        done += 1
        GC.start if done % 40 == 0
        oo << [s, m[:id], m[:words].sum(&:size)]
      end
      done
    end
    parse = Ractor.new(score) do |nxt|
      while (line = Ractor.receive) != :eos
        id, text = line.split("|", 2)
        nxt << { id: id.to_i, words: text.split }
      end
      nxt << :eos
    end
    n.times { parse << Ractor.receive } # relay main's lines inward
    parse << :eos
    raise "stage count" unless score.value == n
    parse.join
    :done
  end
end

expected = 0
N_SUP.times do |s|
  ITEMS_PER_SUP.times do |i|
    id = s * 1000 + i
    text = "alpha beta#{i} #{'gamma ' * (i % 3)}".strip
    expected += text.split.sum(&:size)
    sups[s] << "#{id}|#{text}"
  end
end

got = 0
(N_SUP * ITEMS_PER_SUP).times { got += out.receive[2] }
sups.each { |s| raise "FAIL sup" unless s.value == :done }
raise "FAIL #{got} != #{expected}" unless got == expected
puts "OK nest_pipeline_tree"
