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



namespace ht
{
    const int EMPTY = -1;

    
    
    struct Entry 
    {
        Key_t key;
        Value_t value;
        std::shared_ptr<struct Entry> next;
        
        Entry(Key_t k, Value_t v) : key{k}, value{v}, next{nullptr} {}
    };
    
    
    typedef std::shared_ptr<struct Entry> Partition;
    
    struct PartitionRange
    {
        std::shared_ptr<struct Entry> begin;
        std::shared_ptr<struct Entry> end;
    };
    
    /*
     * params:
     *      - SIZE          how many different entries this hashtable should have
     *      - MAXELEMENTS   how many different keys this hashtable should store at once
     */
    template<int SIZE, int MAXELEMENTS>
    class HashTable 
    {
    public:
        int64_t numElements;
    
        std::array<std::shared_ptr<struct Entry>, SIZE> entries;
        
        std::array<ht::PartitionRange, SIZE> localPartitions;        
        
        HashTable() : numElements{0} 
        {
            std::fill(this->entries.begin(), this->entries.end(), nullptr);
        };
        
        void add(Key_t const new_key, Value_t const new_value)
        {
            auto index = getIndex(new_key);
            
            if(isEntryEmpty(index))
            {
                this->entries[index] = std::make_shared<struct Entry>(new_key, new_value);
            }
            else
            {
                bool done = addToExistingEntryWithSameKey(index, new_key, new_value);
                
                if(!done)
                {
                    insertNewKey(index, new_key, new_value);
                }
            }
            this->numElements++;
            if(isFull())
            {
                this->flush();
            }
        }
        
    private:
        inline bool isFull()
        {
            return this->numElements == MAXELEMENTS;
        }
        inline bool isEntryEmpty(int const index) const
        {
             return this->entries[index] == nullptr;
        }
        inline void insertNewKey(int const index, Key_t const new_key, Value_t const new_value)
        {
            auto newEntry = std::make_shared<struct Entry>(new_key, new_value);
            
            newEntry->next = this->entries[index];
            
            this->entries[index] = newEntry;
        }
        
        inline bool addToExistingEntryWithSameKey(int const index, Key_t const new_key, Value_t const new_value)
        {
            auto curr = this->entries[index];
            
            for(; curr->next != nullptr; curr = curr->next)
            {
                if(curr->key == new_key)
                {
                    curr->value += new_value;
                    return true;
                }
            }
            
            return false;
        }
        
        inline int getIndex(Key_t const key) const
        {
            return key % SIZE;
        }
    public:      
        void flush()
        {
            for(auto currEntry = 0; currEntry < this->entries.size(); ++currEntry)
            {
                if(this->entries[currEntry] == nullptr)
                    continue;
                if(localPartitions[currEntry].begin != nullptr)
                {
                    auto tmp = localPartitions[currEntry].begin;
                    
                    auto last = this->entries[currEntry];
                    while(last->next != nullptr)
                        last = last->next;
                    
                    localPartitions[currEntry].begin = this->entries[currEntry];
                    last->next = tmp;
                    
                    this->entries[currEntry] = nullptr;            
                }
                else
                {
                    localPartitions[currEntry].begin = this->entries[currEntry];
                    localPartitions[currEntry].end = this->entries[currEntry];
                    this->entries[currEntry] = nullptr;
                }
            }
            this->numElements = 0;
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
    static std::array<ht::PartitionRange, SIZE> globalPartitions;
    
    
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
                my_worker.localHashTable->add(this->r[i].key, this->r[i].value);
            }
        }
    };
    
    struct ExchangePartitions
    {
        void operator()(const tbb::blocked_range<int> &numPartition) const
        {
            
            //exchange partitons (Global partitions version)
            for(auto currPartition = numPartition.begin(); currPartition != numPartition.end(); currPartition++)
            {        
                for(auto currWorker = myWorkers.begin(); currWorker != myWorkers.end(); ++currWorker)
                {
                    if(currWorker->localHashTable->localPartitions[currPartition].begin != nullptr)
                    {
                        //Concat local partition with global one 
                        currWorker->localHashTable->localPartitions[currPartition].end->next = globalPartitions[currPartition].begin;
                        globalPartitions[currPartition].begin = currWorker->localHashTable->localPartitions[currPartition].begin;
                        
                        currWorker->localHashTable->localPartitions[currPartition].begin = nullptr;
                        currWorker->localHashTable->localPartitions[currPartition].end = nullptr;
                    }
                    else
                    {
                        continue;
                    }
                }                    
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
                for(auto currEntry = globalPartitions[i].begin; currEntry != nullptr; currEntry = currEntry->next)
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

        // exchange partitions
        tbb::parallel_for(tbb::blocked_range<int>(0, SIZE), ExchangePartitions());

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
std::array<ht::PartitionRange, SIZE> ThreadManager<SIZE, MAXELEMENTS>::globalPartitions;
    
template<int SIZE, int MAXELEMENTS>
typename ThreadManager<SIZE, MAXELEMENTS>::WorkerType ThreadManager<SIZE, MAXELEMENTS>::myWorkers;

template<int SIZE, int MAXELEMENTS>
tbb::concurrent_hash_map<Key_t, Row> ThreadManager<SIZE, MAXELEMENTS>::result;


int main(int argc, char** argv)
{

    if(argc != 4)
    {
        std::cerr << "usage:\n\thyperlike_parallel <num_threads> <num_unique_keys> <num_rows>" << std::endl;
        exit(0);
    }

    int num_threads = std::atoi(argv[1]);
    int num_unique_keys = std::atoi(argv[2]);
    int num_rows = std::atoi(argv[3]);
    
    RelationGenerator generator(num_rows, num_unique_keys, UNIFORM_DISTRIBUTED_KEYS);
    Relation const relation = generator.generateRandomRelation();  
    
    
    //Checks whether relation was build correctly
    assert(relation.isCorrectSum());
    
    ThreadManager<1000, 10'000> manager(num_threads);
    
    timeAndProfileMT_OperationsPerSecond(num_threads, NUM_ROWS, [&](){
        //put aggregations here    
        manager.parallelGroup(relation);
    } );
     
	return 0;
}
