set terminal png size 1600,1200
set output "kafka.writes.png"

set multiplot

set lmargin 6
set rmargin 6
set bmargin 3
set tmargin 0

set size 1, 0.3
set origin 0, 0

set yrange [0:16]
set xrange [0:1000]
set ytics 0,2,16
set xtics ("0" 0, "p10" 100, "p20" 200, "p30" 300, "p40" 400, "p50" 500, "p60" 600, "p70" 700, "p80" 800, "p90" 900, "p100" 1000)

plot "kafka.base.writes.log" using ($1/1000000) title "kafka base (ms)" w l lt rgb "red"

set title 'atomic write to two data partitions'
show title

set size 1, 0.7
set origin 0, 0.3

set bmargin 2
set tmargin 3
set yrange [0:90]
set ytics auto

plot "kafka.tx.writes.log" using ($1/1000000) title "kafka tx (ms)" w l lt rgb "blue",\
     "kafka.base.writes.log" using ($1/1000000) title "kafka base (ms)" w l lt rgb "red"

unset multiplot
