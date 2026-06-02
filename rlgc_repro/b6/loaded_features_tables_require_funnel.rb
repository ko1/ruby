dir = "/tmp/claude-1000/feat8"
Dir.mkdir(dir) rescue nil
NF = 70
NF.times do |i|
  if (i % 5) == 0
    File.write("#{dir}/x#{i}.rb", "raise 'b#{i}'+('a'*40)\n")
  else
    File.write("#{dir}/x#{i}.rb", "module X#{i}; C=('w'*48).freeze; end\nAX#{i}=#{i}.freeze\n")
  end
end
$LOAD_PATH.unshift(dir)
NF.times { |i| autoload "AX#{i}", "x#{i}" if (i % 5) > 0 }

# main churns $" forcing full loaded_features_index + realpath rebuild
churn = Thread.new do
  300.times { $".push("/f#{rand(99999)}.rb"); $".pop; GC.compact rescue nil }
end
hammer = Thread.new { 600.times { GC.start(full_mark: true) } }

# long-lived requiring Ractors (funnel to main) + autoload const_get
long = (0...12).map do |t|
  Ractor.new(dir) do |dir|
    NF.times do |i|
      begin
        require "x#{i}"
        Object.const_get("AX#{i}") rescue nil
      rescue Exception
      end
      GC.start if (i & 3) == 0
    end
    :done
  end
end
# short-lived Ractors that die right after one require -> orphan objspaces
short = Thread.new do
  120.times do |k|
    r = Ractor.new(dir, k % NF) do |dir, i|
      begin; require "x#{i}"; rescue Exception; end
      :x
    end
    r.value
  end
end
long.each(&:value); short.join; churn.join; hammer.join
puts :ok
# Run: RUBY_RACTOR_LOCAL_GC=1 RUBY_GC_STRESS=1 RUBY_GC_HEAP_INIT_SLOTS=2000 ruby --disable-gems this.rb
# Result: 18/18 clean (no crash). Feature tables are single-writer/main-owned via the require funnel.