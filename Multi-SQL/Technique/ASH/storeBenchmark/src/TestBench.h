//
// Created by iron on 2019/12/13.
//

#ifndef STOREBENCHMARK_TESTBENCH_H
#define STOREBENCHMARK_TESTBENCH_H

#include "BenchmarkConfig.h"
#include "DataSource.h"
#include "ExpData.h"
#include "tools/Histogram.h"
#include "tools/Throughput.h"
#include "tools/FixSizeSample.h"
#include "PageCacheStat.h"
#include <boost/dynamic_bitset.hpp>

enum TestStyle {
    SEQ,
    RAND,
    RAND_CHOPPED
};


class TestBench {
public:
    explicit TestBench(DataSource* dataSource, TestStyle style);
    ~TestBench();
    void setChopPara(unsigned int avgWaitMills, unsigned int jitter);
    void setBatchPara(unsigned int avgRecordNum, unsigned int jitter);
    void setOrderKeyPara(unsigned int minOrderKey, unsigned int maxOrderKey);
    int getCurrentRows() const;
    void setCurrentRows(int currentRows);
    int batchInsert(std::list<Batch*>& batchLog, std::list<Record*>& recordLog, unsigned int avgSize, unsigned int jitter);
    int batchInsert(std::list<Batch*>& batchLog, std::list<Record*>& recordLog, unsigned int avgSize, unsigned int jitter, int waitMill, int waitJitter);
    int batchUpdate(std::list<Batch*>& batchLog, std::list<Record*>& recordLog, unsigned int avgSize, unsigned int jitter);
    int batchRead(std::list<Batch*>& batchLog, std::list<Record*>& recordLog, unsigned int avgOrderSize, unsigned int jitter);
    int batchRead(std::list<Batch*>& batchLog, std::list<Record*>& recordLog, unsigned int avgOrderSize, unsigned int jitter, int waitMill, int waitJitter);
    void offerSample(std::vector<int>& sampleOrderKeys);
    virtual void close() {};
    static void clearCache();
    float getCacheRatio();
protected:
    virtual int insert(int rows, FixSizeSample<int>& samples) = 0;
    virtual int scan(unsigned int minOrderKey, unsigned int maxOrderKey, int scanSize, int& retMinOK, int& retMaxOK) = 0;
    virtual PageCacheStat* getPageCacheStat() = 0;
    virtual void getFileMeta(int &outL1, int &outL2, int &outMaxLevel, long& totalSize, long& totalEntries) {};
    Histogram<int> _keyDistribution = Histogram<int>(50000, 100);
    unsigned int _minOrderKey;
    unsigned int _maxOrderKey;
    // data source
    DataSource* source;
private:
    static inline int getRandom(unsigned int avg, unsigned int jitter);
    static bool pairCmp(pair<int, int> a, pair<int, int> b);
    // testbench meta info
    int _currentBatchID;
    long _currentRows;
    long _currentTotalReadRows;
    Op _lastOp;
    TestStyle _style;
    PageCacheStat* _pcStat;
    Throughput _writeThroughput = Throughput(62);
    double _updateHtgSum = 0;
    int _updateCount = 0;
    // operation parameter
    unsigned int _avgWaitMills;
    unsigned int _chopJitter;
    unsigned int _avgRecordNum;
    unsigned int _batchJitter;
};


#endif //STOREBENCHMARK_TESTBENCH_H
