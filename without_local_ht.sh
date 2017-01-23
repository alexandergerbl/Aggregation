#!/bin/bash

gnuplot -e "filename1='global_partitions.num_threads';filename2='hyperlike_shared_parallel.num_threads';filename3='hyperlike_vector_parallel.num_threads';ofilename='num_threads_pre.png'" plot_num_threads_presentation.gnu 


gnuplot -e "filename1='global_partitions.unique';filename2='hyperlike_shared_parallel.unique';filename3='hyperlike_vector_parallel.unique';ofilename='unique_pre.png'" plot_num_threads_presentation.gnu_unique

