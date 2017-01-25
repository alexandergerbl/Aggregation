#ifndef RELATION_HPP
#define RELATION_HPP

#include <vector>
#include <memory>

typedef int64_t Key_t;
typedef int64_t Value_t;


class alignas(64) Row
{
    
public:
    static const int64_t EMPTY = INT64_MIN;
    
    Key_t key;
    Value_t value;
    
    Row() 
    {
        key = EMPTY;
        value = 0;
    }
    
    Row(Key_t key, Value_t value) : key{key}, value{value} {}    
};

class Relation : public std::vector<Row>
{
public:
    
    Value_t getSumOfWholeRelation() const
    {
        Value_t sum = 0;
        for(auto it = this->begin(); it != this->end(); ++it)
            sum += it->value;    
        return sum;
    }
    
    friend std::ostream& operator<<(std::ostream& os, Relation const& relation)
    {
        for(auto& row : relation)
            os << "(" << row.key << ", " << row.value << ")" << std::endl;
    }
    
    /*
    * the sum of all values independent of their key must be the sum from 1 to max_value (should be NUM_ROWS)
    */
    bool isCorrectSum() const
    {
        //little gauss
        auto correct_sum = (this->size() * (this->size() + 1))/2;

        return (correct_sum == this->getSumOfWholeRelation());
    }
};



#endif
