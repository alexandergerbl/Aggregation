#include "RelationGenerator.hpp"

#include <iostream>
#include <cstdint>
#include <vector>
#include <algorithm>
#include <memory>
#include <random>
#include <unordered_map>
#include <cassert>

const int64_t POISSON_VALUE = 120;

/*
* 
* generates pairs of Keys and how often they will appear, dependend on the poisson distribution
* 
* 
*/
std::vector<Group> RelationGenerator::generateGroupedData_POISSON()
{
    std::random_device rd;
    std::mt19937 gen(rd());
    
    //TODO adjust distribution later
    std::poisson_distribution<> d(this->num_rows/this->num_unique_keys);
    
    std::vector<Group> groups;
    
    Key_t tmp_key = 0;
    for(int n=0; n < num_rows;) {
        auto numAppearance = d(gen);
        groups.emplace_back(tmp_key, numAppearance);
        tmp_key++;
        n += numAppearance;
    }
    
    return groups;
}

std::vector<Group> RelationGenerator::generateGroupedData_UNIFORM()
{
    std::vector<Group> groups;
    
    auto numAppearance = this->num_rows/this->num_unique_keys;
    auto count = 0;
    
    for(Key_t tmp_key = 0; tmp_key < this->num_unique_keys; tmp_key++)
    {
        groups.emplace_back(tmp_key, numAppearance);
        count += numAppearance;
    }
    //remainder 
    for(int i = 0; count < num_rows; count++, i++)
    {
        groups[i].numOfAppearance++;
    }
    assert(count == num_rows);
    return groups;
}

Relation RelationGenerator::generateRandomRelation()
{
    std::vector<uint64_t> values(this->num_rows);
    std::iota(values.begin(), values.end(), 1);

    std::random_shuffle(values.begin(), values.end());
    
    Relation result;
    result.resize(this->num_rows);
    
    //generates std::vector<key, numOfAppearance>
//    auto groups = generateGroupedData_POISSON(this->num_rows, POISSON_VALUE);
    std::vector<Group> groups;
    if(this->uniform)
        groups = generateGroupedData_UNIFORM(); 
    else
        groups = generateGroupedData_POISSON();
    {
        int64_t i = 0;
        for(auto& e : groups)
        {
            for(auto k = 0; k < e.numOfAppearance; ++k)
            {
                result[i] = Row(e.key, values[i]);
                i++;
                if(i == this->num_rows)
                    break;
            }
        }
    }
    
    std::random_shuffle(result.begin(), result.end());
  
    return result;
}   
