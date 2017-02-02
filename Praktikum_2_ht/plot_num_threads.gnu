set terminal png
set output ofilename

set grid x y
set key left top title " "
set title "Parallelism"
set xlabel "number of threads"
set xrange [0 : 5]
set ylabel "Mio. Operations/sec"
set xtics 1
set tics out
set autoscale  y

plot filename1 using 1:3 with line title 'no local ht' , filename2 using 1:3 with line title 'hyperlike' , filename3 using 1:3 with line title 'global partitions(lock)'
