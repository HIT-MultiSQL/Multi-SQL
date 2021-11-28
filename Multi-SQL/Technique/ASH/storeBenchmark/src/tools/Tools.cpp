//
// Created by iron on 2019/11/19.
//

#include <cstring>
#include <fstream>

#include "Tools.h"

using namespace std;
void randomizeFile(const char* inputDataPath, const char* outputDataPath) {
    vector<string> data;
    char buffer[200];
    ifstream in;
    in.open(inputDataPath);
    while (in.getline(buffer, 200) && strlen(buffer) > 0) {
        data.emplace_back(string(buffer));
    }
    in.close();

    int total = data.size();
    for (int i = 0; i < data.size(); i++) {
        unsigned int rand = random() % total;
        string tmp = data[i + rand];
        data[i + rand] = data[i];
        data[i] = tmp;
        total --;
    }

    ofstream out;
    out.open(outputDataPath);
    for (auto iter = data.begin(); iter != data.end(); iter++) {
        out << *iter << endl;
    }
    out.close();
}

WeightRandom::WeightRandom(int min, int max, int grainSize): _min(min), _max(max), _grainSize(grainSize) {
    _factor = 0;
    int weight = 1;
    long i;
    for (i = min; i < max - grainSize; i += grainSize) {
        _factor += weight * grainSize;
        weight++;
    }
    if (i < max) {
        _factor += weight * (max - i);
    }
}

int WeightRandom::nextRandom() {
    // rand:     0 ... 99, 100 ... 199, 200 ... 299
    // block:    |   0  |, |           1          |
    // blockPos: |   0  |, |    1    |, |    2    |
    // ret:      0 ... 99, 100 ... 149, 150 ... 199
    unsigned long rand = random() % _factor;
    int blockPos = (int) (rand / _grainSize);
    int block = 0;
    int sum = 0;
    for (int i = 1; i < _max; i++) {
        sum = sum + i;
        if (sum >= blockPos + 1) {
            block = i - 1;
            break;
        }
    }
    unsigned long offset = rand - ((long)blockPos * _grainSize);
    offset = offset / (block + 1);
    return (int)_min + offset + block * _grainSize;
}

void WeightRandom::setRandomSeed(unsigned int seed) {
    srandom(seed);
}

const char * alphaBet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789. ";

std::string randomString(unsigned int length) {
    return randomString(random() % 42 + 10,random() % 4, length);
}

std::string randomString(unsigned int valueRange, unsigned int consecutive, unsigned int length){
    if (valueRange > strlen(alphaBet)) {
        valueRange = strlen(alphaBet);
    }
    if (consecutive == 0) consecutive++;
    std::string ret;
    int current = 0;
    while (current < length) {
        unsigned int offset = random() % valueRange;
        for (int i = 0; i < consecutive; i++) {
            ret += alphaBet[(offset + i) % valueRange];
            current++;
            if (current == length) break;
        }
    }
    return ret;
}