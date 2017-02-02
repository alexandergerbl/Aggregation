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


    

/*
 * returns number of <key, value>-pairs that fit into L1 Cache 
 * 
 */
template<int CACHE_SIZE, typename Key_t, typename Value_t>
constexpr int getMaxElements()
{
    return (CACHE_SIZE / (sizeof(Key_t) + sizeof(Value_t)));
}


constexpr int nextLowerPowerOfTwo(const int number)
{
    return pow(2, log2(number));
}



template<int CACHE_SIZE>
struct HashTable_phase1
{    
    
    
    static const Key_t max_elements =  nextLowerPowerOfTwo(getMaxElements<CACHE_SIZE, Key_t, Value_t>());
    std::array<Row, max_elements> data;
    
    std::array<std::vector<Row>, max_elements> localPartitions;
    
    int size;
    
    int64_t bit_mask;
    
    HashTable_phase1() : size{0}
    {        
        this->bit_mask = this->max_elements-1;        
        
        std::fill(this->data.begin(), this->data.end(), Row());
    };    
    
    inline int64_t hash(Key_t key) const
    // Hash
    {
        uint64_t r=88172645463325252ull^key;
        r^=(r<<13);
        r^=(r>>7);
        return (r^=(r<<17));
    }
    
    //hash key before putting it in here
    inline int getIndex(Key_t key) const
    {
        return key & this->bit_mask;
    }
    
    void flush()
    {
        for(auto entry : this->data)
        {
            this->localPartitions[getIndex(hash(entry.key))].emplace_back(entry);
        }
        this->size = 0;
        std::fill(this->data.begin(), this->data.end(), Row());
    }
    //some entries could be EMPTY
    void final_flush()
    {
        for(auto entry : this->data)
        {
            if(entry.key != Row::EMPTY)
                this->localPartitions[getIndex(hash(entry.key))].emplace_back(entry);
        }
        this->size = 0;
        std::fill(this->data.begin(), this->data.end(), Row());
    }
    
    void insert(Key_t key, Value_t value)
    {
        auto index = getIndex(hash(key));
        
        while(this->data[index].key != Row::EMPTY && this->data[index].key != key)
        {
            index = (index + 1) & this->bit_mask;
        }
        if(this->data[index].key == key)
        {
            this->data[index].value += value;
        }
        else
        {
            this->data[index].key = key;
            this->data[index].value = value;
            
            this->size++;
        
            if(this->size == this->max_elements)
            {
                this->flush();
            }
        }
    }
};

/******************************************************************************************************
 * 
 * 
 * 
 * 
 * HashTable phase 2
 * 
 * 
 * 
 * ****************************************************************************************************/
template<int CACHE_SIZE>
struct HashTable_phase2
{    
    
    
    static const Key_t max_elements =  nextLowerPowerOfTwo(getMaxElements<CACHE_SIZE, Key_t, Value_t>());
    std::array<Row, max_elements> data;
    std::unordered_map<Key_t, Value_t> map;
    bool usedMap;
    
    int size;
    
    int64_t bit_mask;
    
    std::function<void(Key_t, Value_t)> insert;
    
    HashTable_phase2() : size{0}, usedMap{false}
    {        
        this->bit_mask = this->max_elements-1;        
        
        std::fill(this->data.begin(), this->data.end(), Row());
        
        this->insert = [&](Key_t key, Value_t value){
            this->insert_to_cacheHT(key, value);
        };
    };    
    
    inline int64_t hash(Key_t key) const
    // Hash
    {
        uint64_t r=88172645463325252ull^key;
        r^=(r<<13);
        r^=(r>>7);
        return (r^=(r<<17));
    }
    
    //hash key before putting it in here
    inline int getIndex(Key_t key) const
    {
        return key & this->bit_mask;
    }
    
    void insert_to_map(Key_t key, Value_t value)
    {
        if(this->map.count(key) == 0)
            this->map[key] = value;
        else
            this->map[key] += value;
    }
    
    void flush()
    {
        for(auto entry : this->data)
        {
            if(this->map.count(entry.key) == 0)
                this->map[entry.key] = entry.value;
            else
                this->map[entry.key] = entry.value;
        }
        this->size = 0;
        std::fill(this->data.begin(), this->data.end(), Row());
        
        //next inserts into std::unordered_map
        this->insert = [&](Key_t key, Value_t value)
        {
            this->insert_to_map(key, value);
        };
        this->usedMap = true;
    }
    
    //some entries could be EMPTY
    void final_flush()
    {
        for(auto entry : this->data)
        {
            if(entry.key != Row::EMPTY)
                this->localPartitions[getIndex(hash(entry.key))].emplace_back(entry);
        }
        this->size = 0;
        std::fill(this->data.begin(), this->data.end(), Row());
    }
    
    void flush_to_result(std::vector<Row>& result)
    {
        for(auto entry : this->data)
        {
            if(entry.key != Row::EMPTY)
                result.emplace_back(entry);
        }
        this->size = 0;
        std::fill(this->data.begin(), this->data.end(), Row());
    }
    
    void insert_to_cacheHT(Key_t key, Value_t value)
    {
        auto index = getIndex(hash(key));
        
        while(this->data[index].key != Row::EMPTY && this->data[index].key != key)
        {
            index = (index + 1) & this->bit_mask;
        }
        if(this->data[index].key == key)
        {
            this->data[index].value += value;
        }
        else
        {
            this->data[index].key = key;
            this->data[index].value = value;
            
            this->size++;
        
            if(this->size == this->max_elements)
            {
                this->flush();
            }
        }
    }
};

//ThreadWorker contains all data that each thread has to store locally

template<int CACHE_SIZE>
class ThreadWorker
{
public:
    
    HashTable_phase1<CACHE_SIZE> localHashTable_phase1;
    HashTable_phase2<CACHE_SIZE> localHashTable_phase2;
    
};


template<int CACHE_SIZE>
class ThreadManager
{
    /*
    * 
    * GLOBAL PARTITIONS
    * 
    */    
    static const Key_t partition_size = nextLowerPowerOfTwo(getMaxElements<CACHE_SIZE, Key_t, Value_t>());
        
    
    typedef tbb::enumerable_thread_specific<ThreadWorker<CACHE_SIZE>> WorkerType;
    static WorkerType myWorkers;
public:
    
    static Relation result;
    static std::mutex result_mutex;
    

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
   // std::cout << "Chunk size = " << (r.end() - r.begin()) << std::endl;        
            for(int i = r.begin(); i < r.end(); i++)
            {
                my_worker.localHashTable_phase1.insert(this->r[i].key, this->r[i].value);
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
                
                for(auto currWorker = myWorkers.begin(); currWorker != myWorkers.end(); ++currWorker)
                {
            
                    for(auto currEntry = currWorker->localHashTable_phase1.localPartitions[i].begin(); currEntry != currWorker->localHashTable_phase1.localPartitions[i].end(); ++currEntry)
                    {
                        my_worker.localHashTable_phase2.insert(currEntry->key, currEntry->value);
                    }
                    currWorker->localHashTable_phase1.localPartitions[i].clear();
                }
                result_mutex.lock();

                if(my_worker.localHashTable_phase2.usedMap)
                {
                    for(auto& hash_table_entry : my_worker.localHashTable_phase2.map)
                    {
                        result.emplace_back(Row(hash_table_entry.first, hash_table_entry.second));
                    }
                    my_worker.localHashTable_phase2.map.clear();
                }
                else
                {
                    my_worker.localHashTable_phase2.flush_to_result(result);
                }
                
                result_mutex.unlock();
                
            }
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
        //max_elements * num_rows/num_unique_keys
        
        tbb::parallel_for(tbb::blocked_range<int>(0, relation.size()), Phase1(relation));
        
        //flush all localHashtables to partitions
        for(auto it = myWorkers.begin(); it != myWorkers.end(); it++)
        {
            it->localHashTable_phase1.final_flush();
        }

        /*
        * 
        * Phase 2
        *  
        */
        tbb::parallel_for(tbb::blocked_range<int>(0, ThreadManager<CACHE_SIZE>::partition_size), Phase2());

        

    }
};


    
template<int CACHE_SIZE>
typename ThreadManager<CACHE_SIZE>::WorkerType ThreadManager<CACHE_SIZE>::myWorkers;

template<int CACHE_SIZE>
Relation ThreadManager<CACHE_SIZE>::result;

template<int CACHE_SIZE>
std::mutex ThreadManager<CACHE_SIZE>::result_mutex;


template<int CACHE_SIZE>
Value_t getSumOfAllGroupValues()
{
    Value_t finaleSum = 0;
    std::for_each(ThreadManager<CACHE_SIZE>::result.begin(), ThreadManager<CACHE_SIZE>::result.end(), [&](std::pair<Key_t, Row> const& p){
//      std::cout << "(" << p.first << ", " << p.second.value << ")" << std::endl;
        finaleSum += p.second.value;
    });
    return finaleSum;
}

int main(int argc, char** argv)
{

    if(argc != 4)
    {
        std::cerr << "usage:\n\thyperlike <num_threads> <num_unique_keys> <num_rows>" << std::endl;
        exit(0);
    }

    int num_threads = std::atoi(argv[1]);
    int num_unique_keys = std::atoi(argv[2]);
    int num_rows = std::atoi(argv[3]);
    
    bool UNIFORM_DISTRIBUTED_KEYS = true;
    
    RelationGenerator generator(num_rows, num_unique_keys, UNIFORM_DISTRIBUTED_KEYS);
    Relation const relation = generator.generateRandomRelation();  
    
    //Checks whether relation was build correctly
    assert(relation.isCorrectSum());
    
    
    ThreadManager<CACHE_SIZE> manager(num_threads);
    
    timeAndProfileMT_OperationsPerSecond(num_threads, num_unique_keys, num_rows, [&](){
        //put aggregations here    
        manager.parallelGroup(relation);
    } );
    assert((manager.result.size() == num_unique_keys)); 
    assert(manager.result.isCorrectSum(num_rows));
    
            
	return 0;
}
