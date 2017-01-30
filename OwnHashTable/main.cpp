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

#include "include/Relation.hpp"
#include "include/profile.cpp"
#include "include/RelationGenerator.hpp"
#include "include/MorselGenerator.hpp"
#include <bitset>


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

/*
 * hashTable - getIndex via bit_mask
 * 
 */

template<int CACHE_SIZE>
struct HashTable
{    
    static const int64_t EMPTY = INT64_MIN;
    
    struct Entry{
        Key_t key;
        Value_t value;
        
        Entry() : key{EMPTY}, value{EMPTY} {}
        
    };
    static const Key_t max_elements =  nextLowerPowerOfTwo(getMaxElements<CACHE_SIZE, Key_t, Value_t>());
    std::array<struct Entry, max_elements> data;
    std::array<std::vector<struct Entry>, max_elements> partitions;
    
    int size;
    
    int64_t bit_mask;
    
    HashTable() : size{0}
    {        
        this->bit_mask = this->max_elements-1;        
        
        std::fill(this->data.begin(), this->data.end(), Entry());
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
            this->partitions[getIndex(hash(entry.key))].emplace_back(entry);
        }
        this->size = 0;
        std::fill(this->data.begin(), this->data.end(), Entry());
    }
    //some entries could be EMPTY
    void final_flush()
    {
        for(auto entry : this->data)
        {
            if(entry.key != EMPTY)
                this->partitions[getIndex(hash(entry.key))].emplace_back(entry);
        }
        this->size = 0;
        std::fill(this->data.begin(), this->data.end(), Entry());
    }
    

    void insert(Key_t key, Value_t value)
    {
        auto index = getIndex(hash(key));
      
        while(this->data[index].key != EMPTY && this->data[index].key != key)
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


/*
 *
 * HashTable2 - getIndex via module hashtable size
 */

template<int CACHE_SIZE, typename Key_t, typename Value_t>
constexpr int getNumElements()
{
    return (CACHE_SIZE / (sizeof(Key_t) + sizeof(Value_t) + sizeof(std::vector<Row>::iterator)));
}

template<int CACHE_SIZE>
struct HashTable2
{
    static const int64_t EMPTY = -1;
    
        static const Key_t max_elements =  getNumElements<CACHE_SIZE, Key_t, Value_t>();
        std::array<Row, max_elements> data;
        std::array<std::vector<Row>, max_elements> localPartitions;
        
        int size;
        
        HashTable2() : size{0}
        {
            std::fill(this->data.begin(), this->data.end(), Row());
        }
        
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
            return key % this->data.size();
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
                if(entry.key != EMPTY)
                    this->localPartitions[getIndex(hash(entry.key))].emplace_back(entry);
            }
            this->size = 0;
            std::fill(this->data.begin(), this->data.end(), Row());
        }
        
        
        void insert(Key_t key, Value_t value)
        {
            auto index = getIndex(hash(key));
        
            while(this->data[index].key != EMPTY && this->data[index].key != key)
            {
                index = (index + 1) % this->data.size();
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


int main(int argc, char** argv)
{
    
    int num_rows = 10'000'000;
    int num_unique_keys = 1'000'000;
    int UNIFORM_DISTRIBUTED_KEYS = true;
    
    RelationGenerator generator(num_rows, num_unique_keys, UNIFORM_DISTRIBUTED_KEYS);
    Relation const relation = generator.generateRandomRelation();  
    
    /*
     * STL - TEST
     */
    {
        std::unordered_map<Key_t, Value_t> map;
        timeAndProfile("stl::unordered_map", num_rows, [&](){
            for(auto& row : relation)
            {
                if(map.count(row.key) == 0)
                    map[row.key] = row.value;
                else
                    map[row.key] += row.value;
            }
        });
    }
    
    
    std::cout << "\n##########################################\n" << std::endl;
    /*
     * HashTable2 - TEST
     */
        
    {

        HashTable<32> table;
        
        timeAndProfile("HashTable 32B", num_rows, [&](){
            for(auto& row : relation)
            {
                table.insert(row.key, row.value);
            }
            table.final_flush();
        });
    }
    /*
     * HashTable2 - TEST
     */
        
    {

        HashTable<256> table;
        
        timeAndProfile("HashTable 256B", num_rows, [&](){
            for(auto& row : relation)
            {
                table.insert(row.key, row.value);
            }
            table.final_flush();
        });
    }
    /*
     * HashTable2 - TEST
     */
        
    {

        HashTable<4*1024> table;
        
        timeAndProfile("HashTable 4 KiB", num_rows, [&](){
            for(auto& row : relation)
            {
                table.insert(row.key, row.value);
            }
            table.final_flush();
        });
    }
    /*
     * HashTable2 - TEST
     */
        
    {

        HashTable<8*1024> table;
        
        timeAndProfile("HashTable 8 KiB", num_rows, [&](){
            for(auto& row : relation)
            {
                table.insert(row.key, row.value);
            }
            table.final_flush();
        });
    }
    /*
     * HashTable2 - TEST
     */
        
    {

        HashTable<16*1024> table;
        
        timeAndProfile("HashTable 16 KiB", num_rows, [&](){
            for(auto& row : relation)
            {
                table.insert(row.key, row.value);
            }
            table.final_flush();
        });
    }
    
    /*
     * myhashtable - TEST
     */
        
    {
        HashTable<32*1024> table;

        timeAndProfile("HashTable 32 KiB", num_rows, [&](){
            for(auto& row : relation)
            {
                table.insert(row.key, row.value);
            }
            table.final_flush();
        });
        
        int64_t sum = 0;
        for(auto& partition : table.partitions)
        {
            for(auto it = partition.begin(); it != partition.end(); it++)
                    sum += it->value;
        }
        assert((sum == 50000005000000));

    }
    
    
    /*
     * HashTable2 - TEST
     */
        
    {
        HashTable<64*1024> table;
        
        timeAndProfile("HashTable 64 KiB", num_rows, [&](){
            for(auto& row : relation)
            {
                table.insert(row.key, row.value);
            }
            table.final_flush();
        });
    }
    
    /*
     * HashTable2 - TEST
     */
    {
        HashTable<128*1024> table;

        timeAndProfile("HashTable 128 KiB", num_rows, [&](){
            for(auto& row : relation)
            {
                table.insert(row.key, row.value);
            }
            table.final_flush();
        });
    }
    std::cout << "\n##########################################\n" << std::endl;
     /*
     * HashTable2 - TEST
     */
        
    {

        HashTable2<1024> table;
        
        timeAndProfile("HashTable 1 KiB", num_rows, [&](){
            for(auto& row : relation)
            {
                table.insert(row.key, row.value);
            }
            table.final_flush();
        });
    }
     /*
     * HashTable2 - TEST
     */
        
    {

        HashTable2<4*1024> table;
        
        timeAndProfile("HashTable 4 KiB", num_rows, [&](){
            for(auto& row : relation)
            {
                table.insert(row.key, row.value);
            }
            table.final_flush();
        });
    }
    /*
     * HashTable2 - TEST
     */
        
    {

        HashTable2<8*1024> table;
        
        timeAndProfile("HashTable 8 KiB", num_rows, [&](){
            for(auto& row : relation)
            {
                table.insert(row.key, row.value);
            }
            table.final_flush();
        });
    }
    /*
     * HashTable2 - TEST
     */
    {
        HashTable2<16*1024> table;

        timeAndProfile("HashTable(%) 16 KiB", num_rows, [&](){
            for(auto& row : relation)
            {
                table.insert(row.key, row.value);
            }
            table.final_flush();
        });
    }
    
    /*
     * myhashtable - TEST
     */
        
    {
        HashTable2<32*1024> table;

        timeAndProfile("HashTable(%) 32 KiB", num_rows, [&](){
            for(auto& row : relation)
            {
                table.insert(row.key, row.value);
            }
            table.final_flush();
        });
        
        int64_t sum = 0;
        for(auto& partition : table.localPartitions)
        {
            for(auto it = partition.begin(); it != partition.end(); it++)
                    sum += it->value;
        }
        assert((sum == 50000005000000));

    }
    
    /*
     * HashTable2 - TEST
     */
        
    {

        HashTable2<64*1024> table;
        
        timeAndProfile("HashTable(%) 64 KiB", num_rows, [&](){
            for(auto& row : relation)
            {
                table.insert(row.key, row.value);
            }
            table.final_flush();
        });
    }
        
    
    /*
     * HashTable2 - TEST
     */
        
    {
        HashTable2<128*1024> table;
        
        timeAndProfile("HashTable(%) 128 KiB", num_rows, [&](){
            for(auto& row : relation)
            {
                table.insert(row.key, row.value);
            }
            table.final_flush();
        });
    }
    
    
    
    //Checks whether relation was build correctly
    assert(relation.isCorrectSum());
    
    
    return 0;
}
