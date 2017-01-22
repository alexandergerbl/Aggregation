#!/bin/bash

cd Global_Partitions && make && cd ..

cd Hyperlike_shared_parallel && make && cd ..

cd Hyperlike_vector_parallel && make && cd ..

cd No_local_HT && make && cd ..
