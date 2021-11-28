//
// Created by iron on 2019/12/12.
//

#ifndef STOREBENCHMARK_EXPDATA_H
#define STOREBENCHMARK_EXPDATA_H

#include <list>
#include <string>

struct Batch;
struct Record;

class ExpData {
public:
    void writeToDisk(std::string fileName);
    void printExpInfo(std::string title);
    int lsm_conf_maxLevel;
    int lsm_conf_maxL1File;
    int lsm_conf_l2Partition;
    unsigned int rowSize;
    unsigned int actualRedundantBit;
    unsigned int valueRange;
    unsigned int valueConsecutive;
    unsigned int keyField;
    unsigned int intField;
    unsigned int strField;
    unsigned int valueField;
    bool use_bt;
    bool use_lsm;
    bool use_column;
    bool use_snappy;
    bool use_lz4;
    bool isRandom;
    std::list<Batch*> batchDatas;
    std::list<Record*> records;
};

enum Op {
    SCAN,
    APPEND,
    INSERT,
    UPDATE,
    DELETE
};

struct Record{
    long currentRows;
    long timeStampNano;
    int batchID;
    int readRows;
    int writeRows;
    int useTimeMicro;
    Op type;
};

struct Batch{
    long tableSize;
    long latest10Writes;
    long latest30Writes;
    long latest60Writes;
    long deltaCachePage;
    float cacheRatio;
    float htg_scanRatio;
    float htg_updateRatio;
    float htg_updateRatioAvg;
    int keyVariance;
    int batchID;
    int lsm_levels;
    int lsm_l1;
    int lsm_l2;
    int useMicroTimeAvg;
    int log_totalReadRows;
};


#endif //STOREBENCHMARK_EXPDATA_H
