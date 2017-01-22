#ifndef SETTINGS_HPP
#define SETTINGS_HPP

#include <cstdint>

/*
 *	Hashtable -Settings
 **/
constexpr int SIZE = 1'000;
constexpr int MAXELEMENTS = 10'000;

/*
 *	Relation -Settings
 **/
const int64_t NUM_UNIQUE_KEYS = 1'000'000;
const int64_t NUM_ROWS = 10'000'000;
const bool UNIFORM_DISTRIBUTED_KEYS = true;


#endif
