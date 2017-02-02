#ifndef MORSELGENERATOR_HPP
#define MORSELGENERATOR_HPP

class MorselGenerator
{
public:
    int curr;
    int morsel_size;
    int num_rows;
    
    MorselGenerator(int morsel_size, int num_rows) : morsel_size{morsel_size}, num_rows{num_rows}, curr{0}
    {
        
    }
    
    std::pair<int, int> getNextRange(bool & reachedEnd)
    {
        auto start = curr;
        auto end = ((curr + this->morsel_size) < num_rows) ? (curr + this->morsel_size) : (this->num_rows); 
        curr = end;
        
        if(end == num_rows)
            reachedEnd = true;
        
        return std::make_pair(start, end);
    }
};

#endif
