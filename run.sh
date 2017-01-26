#!/bin/bash

NUM_UNIQUE_KEYS=1000000
NUM_ROWS=10000000


#
#  build programs
#
cd Global_Partitions && make && cd ..
cd Hyperlike_vector_parallel && make && cd ..
cd No_local_HT && make && cd ..

programs=(no_local_ht hyperlike_vector_parallel global_partitions)

#
#  change num_threads
#

echo -e "\nNUM_THREADS\n"
for program in ${programs[*]}
do
	echo -e "\t$program"
	rm $program.num_threads
	for((numThreads=1;numThreads<=4;numThreads++))
	do
#		for((rep=0;rep<5;rep++))
#		do
			sudo ./build/$program $numThreads $NUM_UNIQUE_KEYS $NUM_ROWS >> $program.num_threads
#		done
	done
done

gnuplot -e "filename1='no_local_ht.num_threads';filename2='hyperlike_vector_parallel.num_threads';filename3='global_partitions.num_threads';ofilename='num_threads.png'" plot_num_threads.gnu

#
#  change number of unique keys
#
echo -e "\nUNIQUE-Keys\n"
for program in ${programs[*]}
do
	echo -e "\t$program"
	rm $program.unique
	for((num_unique=1000000;num_unique<= $NUM_ROWS ;num_unique=num_unique+1000000))
	do
#		for((rep=0;rep<5;rep++))
#		do
			sudo ./build/$program 4 $num_unique $NUM_ROWS >> $program.unique
#		done
	done
done

gnuplot -e "filename1='no_local_ht.unique';filename2='hyperlike_vector_parallel.unique';filename3='global_partitions.unique';ofilename='unique.png'" plot_num_threads.gnu_unique

#
