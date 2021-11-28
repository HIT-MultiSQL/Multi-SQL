//
// Created by iron on 2019/12/13.
//

#include "TestBench.h"
#include <thread>
#include <chrono>
#include <cmath>
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/variance.hpp>
#include <boost/accumulators/statistics/mean.hpp>
using namespace boost::accumulators;

TestBench::TestBench(DataSource* dataSource, TestStyle style): _style(style) {
    source = dataSource;
    _currentRows = 0;
    _currentBatchID = 0;
    _currentTotalReadRows = 0;
    _minOrderKey = 0;
    _maxOrderKey = 0;
    _avgWaitMills = 0;
    _chopJitter = 0;
    _avgRecordNum = 1;
    _batchJitter = 0;
    _pcStat = nullptr;
    // _updateHtgSum = 0;
    _lastOp = APPEND;
}

TestBench::~TestBench() {
    source->close();
    close();
    delete _pcStat;
}

int TestBench::getRandom(unsigned int avg, unsigned int jitter) {
    if (jitter == 0) return avg;
    if (jitter >= avg) {
        return random() % (avg + jitter + 1);
    } else {
        return (avg - jitter) + random() % (jitter * 2 + 1);
    }
}

void TestBench::setChopPara(unsigned int avgWaitMills, unsigned int jitter) {
    _avgWaitMills = avgWaitMills;
    _chopJitter = jitter;
}

int TestBench::batchRead(std::list<Batch *> &batchLog, std::list<Record *> &recordLog, unsigned int avgOrderSize,
                         unsigned int jitter) {
    return batchRead(batchLog, recordLog, avgOrderSize, jitter, _avgWaitMills, _chopJitter);
}

int TestBench::batchRead(std::list<Batch *> &batchLog, std::list<Record *> &recordLog, unsigned int avgOrderSize,
                         unsigned int jitter, int waitMill, int waitJitter) {
    auto batch = new Batch;
    if (_lastOp != SCAN ) {
        delete _pcStat;
        _pcStat = nullptr;
        if (_currentRows != 0) {
            _pcStat = getPageCacheStat();
        }
    }
    if (_pcStat == nullptr) {
        _pcStat = new PageCacheStat;
    }
    _lastOp = SCAN;

    if (_style == RAND_CHOPPED) {
        int sleepTime = getRandom(waitMill, waitJitter);
        if (sleepTime > 0) {
            std::this_thread::sleep_for (std::chrono::milliseconds(sleepTime));
        }
    }

    int recordSize = getRandom(_avgRecordNum, _batchJitter);
    batch->batchID = _currentBatchID++;
    // update lsm and cache para
    batch->lsm_l1 = 1;
    batch->lsm_l2 = 0;
    batch->lsm_levels = 0;
    batch->tableSize = _pcStat->totalSize;
    long entries;
    getFileMeta(batch->lsm_l1, batch->lsm_l2, batch->lsm_levels, batch->tableSize, entries);
    batch->log_totalReadRows = log(_currentTotalReadRows + 1);
    if (_pcStat->totalPages != 0) {
        batch->cacheRatio = (float) _pcStat->totalCached  / (float) _pcStat->totalPages;
    } else {
        batch->cacheRatio = 1;
    }
    // update buffer para
    batch->latest10Writes = _writeThroughput.latestNAverage(10);
    batch->latest30Writes = _writeThroughput.latestNAverage(30);
    batch->latest60Writes = _writeThroughput.latestNAverage(60);
    // update write para
    batch->htg_updateRatio = 0;
    batch->htg_updateRatioAvg = 0;
    // scan htg
    vector<pair<int, int> > spanRange;
    batch->keyVariance = 0; // TODO find a nice measurement of variance

    int minOK, maxOK = 0;
    auto batchStartTime = chrono::system_clock::now();
    auto recordStartTime = batchStartTime;
    for (int i = 0; i < recordSize; i++) {
        auto record = new Record;
        record->batchID = batch->batchID;
        record->type = SCAN;
        record->currentRows = _currentRows;
        record->writeRows = 0;
        int readOrders = getRandom(avgOrderSize, jitter);
        record->readRows = scan(_minOrderKey, _maxOrderKey, readOrders, minOK, maxOK);
        auto end = chrono::system_clock::now();
        record->timeStampNano = end.time_since_epoch().count(); // nanosecond
        auto duration = chrono::duration_cast<chrono::microseconds>(end - recordStartTime);
        record->useTimeMicro = (int) duration.count();
        recordStartTime = end;
        int minPos = _keyDistribution.getSpanOffset(minOK);
        int maxPos = _keyDistribution.getSpanOffset(maxOK);
        spanRange.emplace_back(make_pair(minPos, maxPos));
        _currentTotalReadRows += record->readRows;
        recordLog.push_back(record);
    }
    auto end = chrono::system_clock::now();
    auto duration = chrono::duration_cast<chrono::microseconds>(end - batchStartTime);
    batch->useMicroTimeAvg = (int)(duration.count() / recordSize);
    int scanSpan = 0;
    int lastSpan = -1;
    sort(spanRange.begin(), spanRange.end(), pairCmp);
    for (auto & i : spanRange) {
        if (i.second > lastSpan) {
            if (i.first > lastSpan) {
                scanSpan += (i.second - i.first + 1);
            } else {
                scanSpan += (i.second - lastSpan);
            }
            lastSpan = i.second;
        }
    }

    batch->htg_scanRatio = (float) scanSpan / (float) _keyDistribution.getSpanNum();

    PageCacheStat* newStat = getPageCacheStat();
    batch->deltaCachePage = newStat->totalCached - _pcStat->totalCached;
    batchLog.push_back(batch);

    delete _pcStat;
    _pcStat = newStat;

    return recordSize;
}

void TestBench::setBatchPara(unsigned int avgRecordNum, unsigned int jitter) {
    _avgRecordNum = avgRecordNum;
    _batchJitter = jitter;
}

int TestBench::batchInsert(std::list<Batch *> &batchLog, std::list<Record *> &recordLog, unsigned int avgSize,
                           unsigned int jitter) {
    return batchInsert(batchLog, recordLog, avgSize, jitter, _avgWaitMills, _chopJitter);
}

int TestBench::batchInsert(std::list<Batch *> &batchLog, std::list<Record *> &recordLog, unsigned int avgSize,
                           unsigned int jitter, int waitMill, int waitJitter) {
    if (!source->hasNext())
        return 0;
    auto batch = new Batch;
    if (_lastOp != SCAN ) {
        delete _pcStat;
        _pcStat = nullptr;
        if (_currentRows != 0) {
            _pcStat = getPageCacheStat();
        }
    }
    if (_pcStat == nullptr) {
        _pcStat = new PageCacheStat;
    }
    _lastOp = _style == SEQ ? APPEND : INSERT;

    if (_style == RAND_CHOPPED) {
        int sleepTime = getRandom(waitMill, waitJitter);
        if (sleepTime > 0) {
            std::this_thread::sleep_for (std::chrono::milliseconds(sleepTime));
        }
    }
    batch->batchID = _currentBatchID++;
    // update lsm and cache para
    batch->lsm_l1 = 1;
    batch->lsm_l2 = 0;
    batch->lsm_levels = 0;
    batch->tableSize = _pcStat->totalSize;
    long entries = _currentRows;
    getFileMeta(batch->lsm_l1, batch->lsm_l2, batch->lsm_levels, batch->tableSize, entries);
    batch->log_totalReadRows = log(_currentTotalReadRows + 1);
    if (_pcStat->totalPages != 0) {
        batch->cacheRatio = (float) _pcStat->totalCached  / (float) _pcStat->totalPages;
    } else {
        batch->cacheRatio = 1;
    }

    // update buffer para
    batch->latest10Writes = _writeThroughput.latestNAverage(10);
    batch->latest30Writes = _writeThroughput.latestNAverage(30);
    batch->latest60Writes = _writeThroughput.latestNAverage(60);
    // update write para
    batch->keyVariance = 0; //TODO find a nice measurement of variance
    batch->htg_scanRatio = 0;
    // calculate bytes per row
    int rowByte;
    if (entries == 0) {
        rowByte = USERINFORM_KEY_LEN + USERINFORM_VALUE_LEN;
    } else {
        rowByte = (int)(batch->tableSize / entries);
    }
    boost::dynamic_bitset<> bitSet(_keyDistribution.getSpanNum());
    FixSizeSample<int> samples(_keyDistribution.getSpanNum());

    int recordSize = getRandom(_avgRecordNum, _batchJitter);
    int totalWriteRows = 0;
    auto batchStartTime = chrono::system_clock::now();
    auto recordStartTime = batchStartTime;
    for (int i = 0; i < recordSize; i++) {
        if (!source->hasNext()) {
            recordSize = i;
            break;
        }
        auto record = new Record;
        record->batchID = batch->batchID;
        record->type = _lastOp;
        record->currentRows = _currentRows;
        record->readRows = 0;
        int writeOrders = getRandom(avgSize, jitter);
        totalWriteRows += writeOrders;
        record->writeRows = insert(writeOrders, samples);
        _currentRows += record->writeRows;
        _writeThroughput.record(record->writeRows * rowByte);
        auto end = chrono::system_clock::now();
        record->timeStampNano = end.time_since_epoch().count(); // nanosecond
        auto duration = chrono::duration_cast<chrono::microseconds>(end - recordStartTime);
        recordStartTime = end;
        record->useTimeMicro = (int) duration.count();
        recordLog.push_back(record);
    }

    auto end = chrono::system_clock::now();
    auto duration = chrono::duration_cast<chrono::microseconds>(end - batchStartTime);
    batch->useMicroTimeAvg = (int)(duration.count() / recordSize);
    samples.fillBitSet(_keyDistribution, bitSet);
    batch->htg_updateRatio = (float) bitSet.count() / (float) _keyDistribution.getSpanNum();
    _updateHtgSum += batch->htg_updateRatio;
    _updateCount ++;
    batch->htg_updateRatioAvg = (float)(_updateHtgSum / _updateCount);

    PageCacheStat* newStat = getPageCacheStat();
    if (newStat != nullptr) {
        batch->deltaCachePage = newStat->totalCached - _pcStat->totalCached;
        delete _pcStat;
        _pcStat = newStat;
    }

    batchLog.push_back(batch);
    return totalWriteRows;
}

void TestBench::setOrderKeyPara(unsigned int minOrderKey, unsigned int maxOrderKey) {
    _minOrderKey = minOrderKey;
    _maxOrderKey = maxOrderKey;
}

void TestBench::offerSample(std::vector<int> &sampleOrderKeys) {
    for (int & sampleOrderKey : sampleOrderKeys) {
        _keyDistribution.offerSample(sampleOrderKey);
    }
}

int TestBench::getCurrentRows() const {
    return _currentRows;
}

void TestBench::setCurrentRows(int currentRows) {
    _currentRows = currentRows;
}

bool TestBench::pairCmp(pair<int, int> a, pair<int, int> b) {
    return a.first < b.first;
}

void TestBench::clearCache() {
    string cmd = "sudo sh ";
    cmd += SCRIPT_DIR;
    cmd += "/clearCache.sh";
    system(cmd.c_str());
    cout << "cache cleared." << endl;
}

float TestBench::getCacheRatio() {
    if (_pcStat == nullptr) {
        return -1;
    } else {
        return (float)(_pcStat->totalCached) / (float)(_pcStat->totalPages);
    }
}
