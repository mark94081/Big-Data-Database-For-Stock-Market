#pragma once
#include <vector>
#include <map>
#include <string>
using namespace std;

// Break a string into vector of substrings based on de
vector<string> split(string text, char delim);

// Process config file for Market Data Retrieval
map<string, string> ProcessConfigData(string config_file);

//writing call back function for storing fetched values in memory
size_t WriteCallback(void* contents, size_t size, size_t nmemb, void* userp);