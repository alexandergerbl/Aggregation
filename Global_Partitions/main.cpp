/*
*
*
*	This program implements a HW aware & parallel aggregation/ group by
*   e.g.
*           select key, sum(value)
*			from R
*			group by key
*/


#include <iostream>
#include <cstdint>
#include <math.h>
#include <unordered_map>
#include <cassert>
#include <algorithm>
#include <array>
#include <chrono>
#include <utility>
#include <array>
#include <mutex>
#include <unistd.h>
#include <cstdlib>

#include <functional>

#include "tbb/task_scheduler_init.h"
#include "tbb/enumerable_thread_specific.h"
#include "tbb/parallel_for.h"
#include "tbb/blocked_range.h"
#include "tbb/concurrent_hash_map.h"

#include "include/Relation.hpp"
#include "include/profile.cpp"
#include "../Settings.hpp"
#include "include/RelationGenerator.hpp"
#include "include/MorselGenerator.hpp"

/**
 * 
 * TODO Hyperlike Implementation of Agregation of tuples
 *
 *      Properties:
 *              - multiple num_threads
 *              - hash-tables for each thread 
 *              - each hash-table has its own local flush-partitions
 *              - flush hash-tables to their local partitions if the hash-table becomes full
 *              - hash-table entries colliding keys will be linked via shared_ptr
 */

/**
 * 
 * 
 * Global hashtable
 * 
 */
template<int SIZE, int MAXELEMENTS>
std::array<std::vector<Row>, SIZE> globalPartitions;

template<int SIZE>
std::array<std::mutex, SIZE> mutex;


namespace ht
{
        
    
    /*
     * params:
     *      - SIZE          how many different entries this hashtable should have
     *      - MAXELEMENTS   how many different keys this hashtable should store at once
     */
    template<int SIZE, int MAXELEMENTS>
    class HashTable 
    {
        int64_t numElements;
    public:
        std::array<std::vector<Row>, SIZE> entries;
        
        HashTable() {};
        
        void add(Row const& row)
        {
            this->entries[getIndex(row.key)].emplace_back(row);
            
            this->numElements++;
            
            if(isFull())
            {
                this->flush();                
            }
        }
        
    private:
        inline bool isFull() const
        {
            return this->numElements == MAXELEMENTS;
        }
        
        inline int getIndex(Key_t const key) const
        {
            return key % SIZE;
        }
    public:
        inline void flush()
        {
            for(auto currPartition = 0; currPartition < globalPartitions<SIZE, MAXELEMENTS>.size(); ++currPartition)
            {
                globalPartitions<SIZE, MAXELEMENTS>[currPartition].reserve(globalPartitions<SIZE, MAXELEMENTS>[currPartition].size() + this->entries[currPartition].size());
                std::move(std::begin(this->entries[currPartition]), std::end(this->entries[currPartition]), std::back_inserter(globalPartitions<SIZE, MAXELEMENTS>[currPartition]));
                this->entries[currPartition].clear();
            }
        }
    };

};


//ThreadWorker contains all data that each thread has to store locally

template<int SIZE, int MAXELEMENTS>
class ThreadWorker
{
public:
    
    std::shared_ptr<ht::HashTable<SIZE, MAXELEMENTS>> localHashTable;
    
    std::unordered_map<Key_t, Value_t> hashTable_secondPhase;
    
    ThreadWorker()
    {
        localHashTable = std::make_shared<ht::HashTable<SIZE, MAXELEMENTS>>();
        
    }
    
};


template<int SIZE, int MAXELEMENTS>
class ThreadManager
{
    /*
    * 
    * GLOBAL PARTITIONS
    * 
    */    
    
    
    
    typedef tbb::enumerable_thread_specific<ThreadWorker<SIZE, MAXELEMENTS>> WorkerType;
    static WorkerType myWorkers;

    static tbb::concurrent_hash_map<Key_t, Row> result;

public:
    int numThreads;
    
    ThreadManager(int num_threads) : numThreads{num_threads} {}
    
    struct Phase1
    {
        Relation const& r;
        Phase1(Relation const& relation) : r{relation}
        {
        }
        void operator()(const tbb::blocked_range<int> &r) const
        {
            typename WorkerType::reference my_worker = myWorkers.local();
            
            for(int i = r.begin(); i != r.end(); ++i)
            {
                my_worker.localHashTable->add(this->r[i]);
            }
        }
    };
    
    struct Phase2
    {
        void operator()(const tbb::blocked_range<int> &r) const
        {
            typename WorkerType::reference my_worker = myWorkers.local();
                    
            for(int i = r.begin(); i != r.end(); ++i)
            {
                for(auto currEntry = globalPartitions<SIZE, MAXELEMENTS>[i].begin(); currEntry != globalPartitions<SIZE, MAXELEMENTS>[i].end(); ++currEntry)
                {
                    if(my_worker.hashTable_secondPhase.count(currEntry->key) == 0)
                        my_worker.hashTable_secondPhase[currEntry->key] = currEntry->value;
                    else
                        my_worker.hashTable_secondPhase[currEntry->key] += currEntry->value;
                }
            }
        }
    };
    
    struct GetResult
    {
        void operator()(const tbb::blocked_range<int> &r) const
        {
            typename WorkerType::reference my_worker = myWorkers.local();

            std::for_each(my_worker.hashTable_secondPhase.begin(), my_worker.hashTable_secondPhase.end(), [&](std::pair<Key_t, Value_t> const& p)
            {

                //check all other workers
                if(result.count(p.first) == 0)
                {
                    tbb::concurrent_hash_map<Key_t, Row>::accessor a;
                    result.insert(a, p.first);
                    a->second.value = p.second;
                }
                else
                {
                    tbb::concurrent_hash_map<Key_t, Row>::accessor a;
                    result.insert(a, p.first);
                    a->second.value = p.second;
                }
            });
        }
    };
    
    void parallelGroup(Relation const& relation)
    {
        //set number of threads to use
        tbb::task_scheduler_init init(this->numThreads);

        /*
        * 
        * Phase 1
        * 
        */
     
        tbb::parallel_for(tbb::blocked_range<int>(0, relation.size()), Phase1(relation));
        
        //flush all localHashtables to partitions
        for(auto it = myWorkers.begin(); it != myWorkers.end(); it++)
        {
            it->localHashTable->flush();
        }

        /*
        * 
        * Phase 2
        *  
        */
        tbb::parallel_for(tbb::blocked_range<int>(0, SIZE), Phase2());

        tbb::parallel_for(tbb::blocked_range<int>(0, SIZE), GetResult());

    }
};


    
template<int SIZE, int MAXELEMENTS>
typename ThreadManager<SIZE, MAXELEMENTS>::WorkerType ThreadManager<SIZE, MAXELEMENTS>::myWorkers;

template<int SIZE, int MAXELEMENTS>
tbb::concurrent_hash_map<Key_t, Row> ThreadManager<SIZE, MAXELEMENTS>::result;


int main(int argc, char** argv)
{
    if(argc != 2)
    {
        std::cerr << "usage:\n\thyperlike_parallel <num_threads>" << std::endl;
        exit(0);
    }

    int num_threads = std::atoi(argv[1]);
    
    RelationGenerator generator(NUM_ROWS, NUM_UNIQUE_KEYS, UNIFORM_DISTRIBUTED_KEYS);
    Relation const relation = generator.generateRandomRelation();  
    
    //Checks whether relation was build correctly
    assert(relation.isCorrectSum());
    
    ThreadManager<SIZE, MAXELEMENTS> manager(num_threads);
    
    timeAndProfileMT("global_partitions (lock)", NUM_ROWS, [&](){
        //put aggregations here    
        manager.parallelGroup(relation);
    } );
     
	return 0;
}