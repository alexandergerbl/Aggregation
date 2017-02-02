#ifndef RELATIONGENERATOR_HPP
#define RELATIONGENERATOR_HPP

#include <vector>
#include <cstdint>
#include <iostream>
#include <memory>

#include "Relation.hpp"


struct Group
{
    Key_t key;
    int numOfAppearance;
    
    Group(Key_t key, int numAppearance) : key{key}, numOfAppearance{numAppearance} {}
    
    std::ostream& operator<<(std::ostream& os)
    {
        os << "(" << this->key << ", " << this->numOfAppearance << ")";
    }
};


class RelationGenerator
{
    int64_t num_rows;
    int num_unique_keys;
    bool uniform;
public:    
    RelationGenerator(int num_rows, int num_unique_keys, bool uniform) : uniform{uniform}, num_unique_keys{num_unique_keys}, num_rows{num_rows} {}
    
    /*
    * 
    * generates pairs of Keys and how often they will appear, dependend on the poisson distribution
    * 
    * 
    */
    std::vector<Group> generateGroupedData_POISSON();

    std::vector<Group> generateGroupedData_UNIFORM();
    
    Relation generateRandomRelation();  
};

#endif
