# NEGATIVE RESULT (crashes:false). Most aggressive harness (b6_enc_max.rb).
# Run: RUBY_RACTOR_LOCAL_GC=1 ruby -I <build>/.ext/x86_64-linux this.rb
# Probes the encoding registry Face-D hazard: rb_encoding_list is a
# rb_gc_register_address'd VM-global that the RLGC local-GC root pass SKIPS,
# but it is only ever (re)assigned by enc_list_update from main at boot, never
# from a worker -> precondition never met. Ran ~286x over baseline/GC_STRESS/
# tiny-heap with 0 crashes.
ENCS = %w[Shift_JIS EUC-JP Big5 GB18030 EUC-KR Windows-1251 ISO-8859-7 CP932
          UTF-16LE UTF-16BE UTF-32LE UTF-32BE EUC-TW Big5-HKSCS GB2312 CP949
          KOI8-R KOI8-U Windows-1252 Windows-1253 Windows-1254 Windows-1255
          ISO-8859-1 ISO-8859-2 ISO-8859-5 ISO-8859-9 ISO-8859-15 macRoman
          macCyrillic macGreek IBM437 IBM866 CP850 CP852 TIS-620 Windows-874
          ISO-2022-JP ISO-2022-JP-2 UTF-7 CESU-8 Emacs-Mule GB1988 stateless-ISO-2022-JP]

holders = 4.times.map do
  Ractor.new do
    keep=[]
    loop do
      m = Ractor.receive
      break if m == :stop
      keep << m
      keep.shift if keep.size > 250
    end
    keep.size
  end
end

hammer  = Thread.new { 1200.times { GC.start(full_mark: true); GC.compact rescue nil } }
hammer2 = Thread.new { 1200.times { GC.start(full_mark: false) } }

base = "Encode THIS string 0123456789 the quick brown fox jumps"
NW = 16
workers = NW.times.map do |wi|
  Ractor.new(wi, ENCS, base, holders) do |wi, encs, base, holders|
    keep=[]; encobjs=[]
    160.times do |it|
      t = encs[(wi + it) % encs.size]
      f = encs[(wi*3 + it*7) % encs.size]
      begin
        e = Encoding.find(t)
        encobjs << e; encobjs.shift if encobjs.size > 12
        u  = base.dup.force_encoding("UTF-8")
        s  = u.encode(t, invalid: :replace, undef: :replace)
        s2 = s.encode(f, invalid: :replace, undef: :replace)
        ("\x80\xA1\xE0\x41"*3).b.force_encoding(e).valid_encoding?
        keep << s2; keep.shift if keep.size > 25
        holders[it % holders.size].send(s2) rescue nil
        holders[(it+1) % holders.size].send(e) rescue nil
      rescue => ex
      end
      if (it % 9) == 0
        sub = Ractor.new(wi, it, encs, base, holders) do |wi, it, encs, base, holders|
          tt = encs[(wi*11 + it*13) % encs.size]
          ss = base.dup.force_encoding("UTF-8").encode(tt, invalid: :replace, undef: :replace) rescue base
          holders[it % holders.size].send(ss) rescue nil
          ss
        end
        sub.value rescue nil   # orphan objspace
      end
      GC.start   if (it & 3)  == 0
      GC.compact if (it & 15) == 7
    end
    [:done, wi]
  end
end

workers.each { |w| w.value rescue nil }
holders.each { |h| h.send(:stop) }
hammer.join; hammer2.join
puts "OK enc=#{Encoding.list.size} held=#{holders.map{|h| h.value rescue -1}.inspect}"