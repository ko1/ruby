# sorted 2 系列の merge join を 1 worker で行い、対の数と合計を逐次参照と比較
# axes: 1 worker, copy in / value out, GC.start before join
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

la = (0...30).map { |i| [i * 2 % 20, i] }.sort # 重複 key あり
lb = (0...24).map { |i| [i * 3 % 20, i * 10] }.sort
exp_cnt = 0
exp_sum = 0
la.each do |ka, va|
  lb.each do |kb, vb|
    if ka == kb
      exp_cnt += 1
      exp_sum += va + vb
    end
  end
end

r = Ractor.new(la, lb) do |a, b|
  cnt = 0
  sum = 0
  i = 0
  j = 0
  while i < a.size && j < b.size
    if a[i][0] < b[j][0]
      i += 1
    elsif a[i][0] > b[j][0]
      j += 1
    else
      # 同一 key の run 同士を直積
      k = a[i][0]
      i2 = i
      i2 += 1 while i2 < a.size && a[i2][0] == k
      j2 = j
      j2 += 1 while j2 < b.size && b[j2][0] == k
      (i...i2).each do |x|
        (j...j2).each do |y|
          cnt += 1
          sum += a[x][1] + b[y][1]
        end
      end
      i = i2
      j = j2
    end
  end
  [cnt, sum]
end
GC.start
cnt, sum = r.value
raise "cnt=#{cnt} exp=#{exp_cnt}" unless cnt == exp_cnt
raise "sum=#{sum} exp=#{exp_sum}" unless sum == exp_sum
puts "OK b32_join_sortmerge"
