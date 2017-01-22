#!/bin/bash

NUM_THREADS=4

#
#  build programs
#
cd Global_Partitions && make && cd ..
cd Hyperlike_shared_parallel && make && cd ..
cd Hyperlike_vector_parallel && make && cd ..
cd No_local_HT && make && cd ..


#
#  execute programs
#
sudo ./build/global_partitions $NUM_THREADS
sudo ./build/hyperlike_shared_parallel $NUM_THREADS
sudo ./build/hyperlike_vector_parallel $NUM_THREADS
sudo ./build/no_local_ht $NUM_THREADS

