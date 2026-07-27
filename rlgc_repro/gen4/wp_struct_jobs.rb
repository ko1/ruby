# gen4 worker-pool: job payloads are plain objects with ivars (and Structs);
# workers mutate their copies and reply with derived record objects.
# axes: transfer=copy, GC=none, exceptions=none, payload=objects with ivars
class JobRec
  attr_accessor :id, :title, :weights, :meta
  def initialize(id, title, weights, meta)
    @id, @title, @weights, @meta = id, title, weights, meta
  end
  def score = @weights.sum + @title.size + @meta.size
end

Result = Struct.new(:job_id, :worker, :score, :tag)

N_WORKERS = 4
N_JOBS = 300

results = Ractor::Port.new
workers = N_WORKERS.times.map do |wid|
  Ractor.new(results, wid) do |res, id|
    done = 0
    while (job = Ractor.receive) != :stop
      job.weights.map! { |w| w * 2 }
      job.title << "!"
      done += 1
      res << Result.new(job.id, id, job.score, "t#{job.id % 3}")
    end
    done
  end
end

expected = 0
N_JOBS.times do |i|
  rec = JobRec.new(i, "task-#{i}", [i % 5, i % 7, 3], { pri: i % 3 })
  expected += rec.weights.sum * 2 + rec.title.size + 1 + rec.meta.size
  workers[i % N_WORKERS] << rec
end
workers.each { |w| w << :stop }

got = 0
N_JOBS.times do
  r = results.receive
  raise "FAIL type" unless r.is_a?(Result) && r.tag.start_with?("t")
  got += r.score
end
raise "FAIL done" unless workers.sum(&:value) == N_JOBS
raise "FAIL score #{got} != #{expected}" unless got == expected
puts "OK wp_struct_jobs"
