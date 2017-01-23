#!/bin/bash

NUM_UNIQUE_KEYS=1000000
NUM_ROWS=10000000


#
#  build programs
#
cd Global_Partitions && make && cd ..
cd Hyperlike_shared_parallel && make && cd ..
cd Hyperlike_vector_parallel && make && cd ..
cd No_local_HT && make && cd ..

programs=(no_local_ht hyperlike_shared_parallel hyperlike_vector_parallel global_partitions)

#
#  change num_threads
#

echo -e "\nNUM_THREADS\n"
for program in ${programs[*]}
do
	echo -e "\t$program"
	rm $program.data
	for((numThreads=1;numThreads<=4;numThreads++))
	do
#		for((rep=0;rep<5;rep++))
#		do
			sudo ./build/$program $numThreads $NUM_UNIQUE_KEYS $NUM_ROWS >> $program.data
#		done
	done
done

gnuplot -e "filename1='no_local_ht.data';filename2='hyperlike_shared_parallel.data';filename3='hyperlike_vector_parallel.data';filename4='global_partitions.data';ofilename='num_threads.png'" plot_num_threads.gnu

#
#  change number of unique keys
#
echo -e "\nUNIQUE-Keys\n"

