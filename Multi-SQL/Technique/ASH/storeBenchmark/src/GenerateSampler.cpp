//
// Created by iron on 2020/1/13.
//

#include <thread>
#include <cmath>
#include <boost/algorithm/string.hpp>
#include "GenerateSampler.h"
#include "ExpData.h"
#include "TestBench.h"
#include "CombinedSource.h"
#include "WiredTigerBench.h"
#include "RocksdbBench.h"
#include "ColumnStoreSource.h"
#include "WiredTigerColumnBench.h"
#include "UserInformSource.h"
#include "UserBehaviorSource.h"
#include "UserInformColumnSource.h"
#include "UserBehaviorColumnSource.h"

//#define SAMPLER_DEBUG
/**
 * 执行wiredtiger和rocksdb行式存储生成数据的B+和LSM测试
 * @param path
 * @param style
 * @param serialNum
 */
void GenerateSampler::runExperiment(string& path, TestStyle style, int serialNum) {
    bool isRandom = false;
    if (style == RAND || style == RAND_CHOPPED) {
        isRandom = true;
    }

#ifdef SAMPLER_DEBUG
    unsigned int totalLength = 640;
    unsigned int valueRange = 26;
    unsigned int consecutive = 1;
#else
    unsigned int totalLength = 8 + random() % 117; // 一行数据的长度
    unsigned int valueRange = 10 + random() % 53; // 数据的取值范围
    unsigned int consecutive = 1 + random() % 4; // 生成连续字母字符串，提高重复概率
#endif
    // total 2GB data, at least 1M rows, and 10000 scan
    unsigned int totalRows = (1 << 31) / totalLength + 1; //数据的行数
    if (isRandom) {
        totalRows = totalRows * 3 / 4;
    }

    unsigned int singleScanSize = 200; // 每次scan的行数
    unsigned int totalScan = 5000; // 5000次scan
    unsigned int batchSize = 4000; // 批插入，平均每次插入4000行
    unsigned int leadKeySize = 30; // 前缀索引
    time_t now;

#ifdef SAMPLER_DEBUG
    KeySource* keySource = new Key4Source(5);
    ValueSource* valueSource = new Value1Source(totalLength - 4);
    keySource->setRedundancy(15);
#else
    KeySource* keySource = getKeySource(totalLength); // 生成keysource
    ValueSource* valueSource = getValueSource(totalLength, valueRange, consecutive); // 生成valuesource
    keySource->setRedundancy(random()% 15 + 1);
#endif

    // 记录日志
    auto* log = new ExpData();
//    log->valueRange = valueRange;
//    log->valueConsecutive = consecutive;
//    log->isRandom = isRandom;
//    log->use_bt = true;
//    log->rowSize = totalLength + keySource->getKeyLength();
//    log->keyField = keySource->getKeyFieldsNum();
//    log->valueField = valueSource->getValueFieldsNum();
//    now = time(nullptr);
//    cout << ctime(&now);
//    log->printExpInfo("WT " + to_string(serialNum));
//    // 生成datasource
//    DataSource* dataSourceWt = new CombinedSource(keySource, valueSource, isRandom, singleScanSize, totalRows);
//    log->actualRedundantBit = keySource->getActualRedundancy(); // 控制真实信息量
//
//    // 将数据插入到bench中
//    TestBench* benchWT = new WiredTigerBench(path + "/wttest" + to_string(serialNum), true, dataSourceWt, style);
//    benchWT->setBatchPara(1, 0); // 不进行批处理
//    // 给用户展示进度
    double writePercentage = 0;
    double scanPercentage = 0;
    int lastWriteOutput = 0;
    int lastScanOutput = 0;
    unsigned int currentRows = 0; // 已有多少行
    unsigned int insertRows = 0; // 已插入多少行
//
//    // 执行四次先写后读
//    for (int i = 0; i < 4; i++) {
//        auto startTime = chrono::system_clock::now();
//        while(writePercentage < 25 * (i + 1) && (insertRows =  benchWT->batchInsert(log->batchDatas, log->records, batchSize, batchSize - 1)) != 0){
//            currentRows += insertRows;
//            writePercentage = currentRows * 1.0 / totalRows * 100;
//            if (writePercentage - lastWriteOutput > 1) {
//                lastWriteOutput++;
//                now = time(nullptr);
//                cout << ctime(&now) << "write " << writePercentage << "%" << endl;
//                auto endTime = chrono::system_clock::now();
//                auto duration = chrono::duration_cast<chrono::minutes>(endTime - startTime);
//                if (duration.count() > 25) {
//                    // 超过25分钟则跳过本批剩余数据
//                    currentRows = ceil(totalRows * (25.0 * ( i + 1 ) / 100));
//                    break;
//                }
//            }
//        }
//#ifndef SAMPLER_DEBUG
//        waitUntilNextFlush();
//#endif
//        startTime = chrono::system_clock::now();
//        while(scanPercentage < 25 * (i + 1)) {
//            if (random() % 10 < 2) {
//                benchWT->batchRead(log->batchDatas, log->records, 1, 0);
//            } else {
//                benchWT->batchRead(log->batchDatas, log->records, leadKeySize, leadKeySize - 1);
//            }
//            scanPercentage += 1.0 / totalScan * 100;
//            if (scanPercentage - lastScanOutput > 1) {
//                lastScanOutput++;
//                now = time(nullptr);
//                cout << ctime(&now) << "scan " << scanPercentage << "%" << endl;
//                auto endTime = chrono::system_clock::now();
//                auto duration = chrono::duration_cast<chrono::minutes>(endTime - startTime);
//                if (duration.count() > 25) {
//                    // 读超过25分钟则降级
//                    totalScan = 1000;
//                }
//            }
//        }
//        totalScan = 5000;
//    }
//    log->writeToDisk(path + "/wttest" + to_string(serialNum) +".log");
//    benchWT->close();
//    delete log;
//    delete keySource;
//    delete valueSource;
//    delete dataSourceWt;


#ifdef SAMPLER_DEBUG
    totalLength = 640;
#else
    totalLength = 8 + random() % 1017;
#endif
    // total 2GB data, at least 1M rows, and 10000 scan
    totalRows = (1 << 31) / totalLength + 1;
    if (isRandom) {
        totalRows = totalRows * 3 / 4;
    }
#ifdef SAMPLER_DEBUG
    keySource = new Key1Source();
    valueSource = new Value1Source(totalLength - 4);
    keySource->setRedundancy(15);
#else
    keySource = getKeySource(totalLength);
    valueSource = getValueSource(totalLength, valueRange, consecutive);
    keySource->setRedundancy(random()% 15 + 1);
#endif
    log = new ExpData();
    log->valueRange = valueRange;
    log->valueConsecutive = consecutive;
    log->isRandom = isRandom;
    log->use_lsm = true;
    log->rowSize = totalLength + keySource->getKeyLength();
    log->keyField = keySource->getKeyFieldsNum();
    log->valueField = valueSource->getValueFieldsNum();
    now = time(nullptr);
    cout << ctime(&now);
    log->printExpInfo("RocksDB " + to_string(serialNum));
    DataSource* dataSourceR = new CombinedSource(keySource, valueSource, isRandom, singleScanSize, totalRows);
    log->actualRedundantBit = keySource->getActualRedundancy();

    rocksdb::Options options;
    TestBench* benchR = new RocksdbBench(path + "/rtest" + to_string(serialNum), true, options, dataSourceR, style);
    writePercentage = 0;
    scanPercentage = 0;
    lastWriteOutput = 0;
    lastScanOutput = 0;
    currentRows = 0;
    insertRows = 0;

    for (int i = 0; i < 4; i++) {
        auto startTime = chrono::system_clock::now();
        while(writePercentage < 25 * (i + 1) && (insertRows = benchR->batchInsert(log->batchDatas, log->records, batchSize, batchSize - 1)) != 0){
            currentRows += insertRows;
            writePercentage = currentRows * 1.0 / totalRows * 100;
            if (writePercentage - lastWriteOutput > 1) {
                lastWriteOutput++;
                now = time(nullptr);
                cout << ctime(&now) << "write " << writePercentage << "%" << endl;
                auto endTime = chrono::system_clock::now();
                auto duration = chrono::duration_cast<chrono::minutes>(endTime - startTime);
                if (duration.count() > 25) {
                    // 超过25分钟则跳过本批剩余数据
                    currentRows = ceil(totalRows * (25.0 * ( i + 1 ) / 100));
                    break;
                }
            }
        }
#ifndef SAMPLER_DEBUG
        waitUntilNextFlush();
#endif
        startTime = chrono::system_clock::now();
        while(scanPercentage < 25 * (i + 1)) {
            if (random() % 10 < 2) {
                benchR->batchRead(log->batchDatas, log->records, 1, 0);
            } else {
                benchR->batchRead(log->batchDatas, log->records, leadKeySize, leadKeySize - 1);
            }
            scanPercentage += 1.0 / totalScan * 100;
            if (scanPercentage - lastScanOutput > 1) {
                lastScanOutput++;
                now = time(nullptr);
                cout << ctime(&now) << "scan " << scanPercentage << "%" << endl;
                auto endTime = chrono::system_clock::now();
                auto duration = chrono::duration_cast<chrono::minutes>(endTime - startTime);
                if (duration.count() > 25) {
                    // 读超过25分钟则降级
                    totalScan = 1000;
                }
            }
        }
        totalScan = 5000;
    }
    log->writeToDisk(path + "/rtest" + to_string(serialNum) +".log");
    benchR->close();
    delete log;
    delete keySource;
    delete valueSource;
    delete dataSourceR;
}

KeySource *GenerateSampler::getKeySource(unsigned int& rowLength) {
    // key1:25 key2:30 key3:25 key4:10, key5:10
    int prob[] = {25, 55, 80, 90, 100};
    int r = (int)(random() % 100);
    KeySource* ret = nullptr;
    if (r >= prob[4] && rowLength > 17) {
        rowLength = rowLength - 16;
        unsigned int maxKeyStringLen = rowLength / 4;
        if (maxKeyStringLen > 32) {
            maxKeyStringLen = 32;
        }
        unsigned int keyStringLen = 1;
        if (maxKeyStringLen > 1) {
            keyStringLen = random() % maxKeyStringLen + 1;
        }
        ret = new Key5Source(keyStringLen);
        rowLength = rowLength - keyStringLen;
        return ret;
    }
    if (r >= prob[3] && rowLength > 13) {
        rowLength = rowLength - 12;
        unsigned int maxKeyStringLen = rowLength / 4;
        if (maxKeyStringLen > 32) {
            maxKeyStringLen = 32;
        }
        unsigned int keyStringLen = 1;
        if (maxKeyStringLen > 1) {
            keyStringLen = random() % maxKeyStringLen + 1;
        }
        ret = new Key4Source(keyStringLen);
        rowLength = rowLength - keyStringLen;
        return ret;
    }
    if (r >= prob[2] && rowLength > 12) {
        rowLength = rowLength - 12;
        ret = new Key3Source();
        return ret;
    }
    if (r >= prob[1] && rowLength > 8) {
        rowLength = rowLength - 8;
        ret = new Key2Source();
        return ret;

    }
    rowLength = rowLength - 4;
    ret = new Key1Source();
    return ret;
}

ValueSource *GenerateSampler::getValueSource(unsigned int length, unsigned int valueRange, unsigned int consecutive) {
    // value1:15 value2:15 value3:15 value4:10 value6:10 value8:10 value10:10 value12:5 value16:5 value20:5
    int prob[] = {15, 30, 45, 55, 65, 75, 85, 90, 95, 100};
    int r = (int)(random() % 100);
    ValueSource* ret = nullptr;
    if (r >= prob[9] && length >= 80) {
        ret = new Value20Source(length);
        ret->setRandomPara(valueRange, consecutive);
        return ret;
    }
    if (r >= prob[8] && length >= 64) {
        ret = new Value16Source(length);
        ret->setRandomPara(valueRange, consecutive);
        return ret;
    }
    if (r >= prob[7] && length >= 48) {
        ret = new Value12Source(length);
        ret->setRandomPara(valueRange, consecutive);
        return ret;
    }
    if (r >= prob[6] && length >= 40) {
        ret = new Value10Source(length);
        ret->setRandomPara(valueRange, consecutive);
        return ret;
    }
    if (r >= prob[5] && length >= 32) {
        ret = new Value8Source(length);
        ret->setRandomPara(valueRange, consecutive);
        return ret;
    }
    if (r >= prob[4] && length >= 24) {
        ret = new Value6Source(length);
        ret->setRandomPara(valueRange, consecutive);
        return ret;
    }
    if (r >= prob[3] && length >= 16) {
        ret = new Value4Source(length);
        ret->setRandomPara(valueRange, consecutive);
        return ret;
    }
    if (r >= prob[2] && length >= 12) {
        ret = new Value3Source(length);
        ret->setRandomPara(valueRange, consecutive);
        return ret;
    }
    if (r >= prob[1] && length >= 8) {
        ret = new Value2Source(length);
        ret->setRandomPara(valueRange, consecutive);
        return ret;
    }
    ret = new Value1Source(length);
    ret->setRandomPara(valueRange, consecutive);
    return ret;
}

void GenerateSampler::runUserInform(string &userInformPath, string &path, TestStyle style, int serialNum) {
    bool isRandom = false;
    int wtRunTurn = 4;
    if (style == RAND || style == RAND_CHOPPED) {
        isRandom = true;
        wtRunTurn = 3;
    }
    unsigned int totalLength = 39;
    unsigned int totalRows = 10000;

    unsigned int valueRange = 100;
    unsigned int consecutive = 4;
    unsigned int totalScan = 50;
    unsigned int batchSize = 40;

    time_t now;

    auto* log = new ExpData();
    log->actualRedundantBit = 0;
    log->valueRange = valueRange;
    log->valueConsecutive = consecutive;
    log->isRandom = isRandom;
    log->use_bt = true;
    log->rowSize = totalLength;
    log->keyField = 1;
    log->valueField = 5;
    now = time(0);
    cout << ctime(&now);
    log->printExpInfo("WT " + to_string(serialNum));
    DataSource* dataSourceWt = new UserInformSource(userInformPath.c_str());
    TestBench* benchWT = new WiredTigerBench(path + "/wttest" + to_string(serialNum), true, dataSourceWt, style);
//    benchWT->setBatchPara(1, 0);
    double writePercentage = 0;
    double scanPercentage = 0;
    int lastWriteOutput = 0;
    int lastScanOutput = 0;

    int currentRows = 0;
    int insertRows = 0;
    for (int i = 0; i < wtRunTurn; i++) {
        while(writePercentage < 25 * (i + 1) && (insertRows = benchWT->batchInsert(log->batchDatas, log->records, batchSize, batchSize - 1)) != 0){
            currentRows += insertRows;
            writePercentage = currentRows * 1.0 / totalRows * 100;
            if (writePercentage - lastWriteOutput > 1) {
                lastWriteOutput++;
                now = time(0);
                cout << ctime(&now) << "write " << writePercentage << "%" << endl;
            }
        }
        waitUntilNextFlush();
        while(scanPercentage < 25 * (i + 1)) {
            benchWT->batchRead(log->batchDatas, log->records, batchSize, batchSize - 1);
            scanPercentage += 1.0 / totalScan * 100;
            if (scanPercentage - lastScanOutput > 1) {
                lastScanOutput++;
                now = time(0);
                cout << ctime(&now) << "scan " << scanPercentage << "%" << endl;
            }
        }
    }
    cout << "WT:!!!" << endl;
    log->writeToDisk(path + "/wttest" + to_string(serialNum) +".log");
    benchWT->close();
    delete log;
    delete dataSourceWt;

    totalScan = 50;
    totalLength = 39;
    log = new ExpData();
    log->actualRedundantBit = 0;
    log->valueRange = valueRange;
    log->valueConsecutive = consecutive;
    log->isRandom = isRandom;
    log->use_lsm = true;
    log->rowSize = totalLength;
    log->keyField = 1;
    log->valueField = 5;
    now = time(0);
    cout << ctime(&now);
    log->printExpInfo("RocksDB " + to_string(serialNum));
    DataSource* dataSourceR = new UserInformSource(userInformPath.c_str());
    rocksdb::Options options;
    TestBench* benchR = new RocksdbBench(path + "/rtest" + to_string(serialNum), true, options, dataSourceR, style);
    benchR->setBatchPara(1,0);
    writePercentage = 0;
    scanPercentage = 0;
    lastWriteOutput = 0;
    lastScanOutput = 0;
    insertRows = 0;
    currentRows = 0;
    for (int i = 0; i < 4; i++) {
        while(writePercentage < 25 * (i + 1) && (insertRows = benchR->batchInsert(log->batchDatas, log->records, batchSize, batchSize - 1)) != 0){
            currentRows += insertRows;
            writePercentage = currentRows * 1.0 / totalRows * 100;
            if (writePercentage - lastWriteOutput > 1) {
                lastWriteOutput++;
                now = time(0);
                cout << ctime(&now) << "write " << writePercentage << "%" << endl;
            }
        }
        waitUntilNextFlush();
        while(scanPercentage < 25 * (i + 1)) {
            benchR->batchRead(log->batchDatas, log->records, batchSize, batchSize - 1);
            scanPercentage +=  1.0 / totalScan * 100;
            if (scanPercentage - lastScanOutput > 1) {
                lastScanOutput++;
                now = time(0);
                cout << ctime(&now) << "scan " << scanPercentage << "%" << endl;
            }
        }
    }
    cout << "ROCKSDB:!!!" << endl;
    log->writeToDisk(path + "/rtest" + to_string(serialNum) +".log");
    benchR->close();
    delete log;
    delete dataSourceR;
}

void GenerateSampler::runUserBehavior(string &userBehaviorPath, string &path, TestStyle style, int serialNum) {
    bool isRandom = false;
    int wtRunTurn = 4;
    if (style == RAND || style == RAND_CHOPPED) {
        isRandom = true;
        wtRunTurn = 3;
    }
    unsigned int totalLength = 42;
    unsigned int totalRows = 100000;

    unsigned int valueRange = 100;
    unsigned int consecutive = 4;
    unsigned int totalScan = 500;
    unsigned int batchSize = 400;

    time_t now;

    auto* log = new ExpData();
    log->actualRedundantBit = 0;
    log->valueRange = valueRange;
    log->valueConsecutive = consecutive;
    log->isRandom = isRandom;
    log->use_bt = true;
    log->rowSize = totalLength;
    log->keyField = 1;
    log->valueField = 4;
    now = time(0);
    cout << ctime(&now);
    log->printExpInfo("WT " + to_string(serialNum));
    DataSource* dataSourceWt = new UserBehaviorSource(userBehaviorPath.c_str());
    TestBench* benchWT = new WiredTigerBench(path + "/wttest" + to_string(serialNum), true, dataSourceWt, style);
//    benchWT->setBatchPara(1, 0);
    double writePercentage = 0;
    double scanPercentage = 0;
    int lastWriteOutput = 0;
    int lastScanOutput = 0;

    int currentRows = 0;
    int insertRows = 0;
    for (int i = 0; i < wtRunTurn; i++) {
        while(writePercentage < 25 * (i + 1) && (insertRows = benchWT->batchInsert(log->batchDatas, log->records, batchSize, batchSize - 1)) != 0){
            currentRows += insertRows;
            writePercentage = currentRows * 1.0 / totalRows * 100;
            if (writePercentage - lastWriteOutput > 1) {
                lastWriteOutput++;
                now = time(0);
                cout << ctime(&now) << "write " << writePercentage << "%" << endl;
            }
        }
        waitUntilNextFlush();
        while(scanPercentage < 25 * (i + 1)) {
            benchWT->batchRead(log->batchDatas, log->records, batchSize, batchSize - 1);
            scanPercentage += 1.0 / totalScan * 100;
            if (scanPercentage - lastScanOutput > 1) {
                lastScanOutput++;
                now = time(0);
                cout << ctime(&now) << "scan " << scanPercentage << "%" << endl;
            }
        }
    }
    cout << "WT:!!!" << endl;
    log->writeToDisk(path + "/wttest" + to_string(serialNum) +".log");
    benchWT->close();
    delete log;
    delete dataSourceWt;

    totalScan = 500;
    totalLength = 42;
    log = new ExpData();
    log->actualRedundantBit = 0;
    log->valueRange = valueRange;
    log->valueConsecutive = consecutive;
    log->isRandom = isRandom;
    log->use_lsm = true;
    log->rowSize = totalLength;
    log->keyField = 1;
    log->valueField = 4;
    now = time(0);
    cout << ctime(&now);
    log->printExpInfo("RocksDB " + to_string(serialNum));
    DataSource* dataSourceR = new UserBehaviorSource(userBehaviorPath.c_str());
    rocksdb::Options options;
    TestBench* benchR = new RocksdbBench(path + "/rtest" + to_string(serialNum), true, options, dataSourceR, style);
    benchR->setBatchPara(1,0);
    writePercentage = 0;
    scanPercentage = 0;
    lastWriteOutput = 0;
    lastScanOutput = 0;
    insertRows = 0;
    currentRows = 0;
    for (int i = 0; i < 4; i++) {
        while(writePercentage < 25 * (i + 1) && (insertRows = benchR->batchInsert(log->batchDatas, log->records, batchSize, batchSize - 1)) != 0){
            currentRows += insertRows;
            writePercentage = currentRows * 1.0 / totalRows * 100;
            if (writePercentage - lastWriteOutput > 1) {
                lastWriteOutput++;
                now = time(0);
                cout << ctime(&now) << "write " << writePercentage << "%" << endl;
            }
        }
        waitUntilNextFlush();
        while(scanPercentage < 25 * (i + 1)) {
            benchR->batchRead(log->batchDatas, log->records, batchSize, batchSize - 1);
            scanPercentage +=  1.0 / totalScan * 100;
            if (scanPercentage - lastScanOutput > 1) {
                lastScanOutput++;
                now = time(0);
                cout << ctime(&now) << "scan " << scanPercentage << "%" << endl;
            }
        }
    }
    cout << "ROCKSDB:!!!" << endl;
    log->writeToDisk(path + "/rtest" + to_string(serialNum) +".log");
    benchR->close();
    delete log;
    delete dataSourceR;
}
/**
 * 执行wiredtiger和rocksdb行式存储TPC数据的B+和LSM测试
 * @param lineItemPath
 * @param path
 * @param style
 * @param serialNum
 */
void GenerateSampler::runTPC(string &lineItemPath, string &path, TestStyle style, int serialNum) {
    bool isRandom = false;
    int wtRunTurn = 4;
    if (style == RAND || style == RAND_CHOPPED) {
        isRandom = true;
        wtRunTurn = 3;
    }

    unsigned int totalLength = 121;
    unsigned int totalRows = 11997996;

    unsigned int valueRange = 54;
    unsigned int consecutive = 4;
    unsigned int totalScan = 5000;
    unsigned int batchSize = 4000;
    time_t now;

    auto* log = new ExpData();
    log->actualRedundantBit = 0;
    log->valueRange = valueRange;
    log->valueConsecutive = consecutive;
    log->isRandom = isRandom;
    log->use_bt = true;
    log->rowSize = totalLength;
    log->keyField = 4;
    log->valueField = 12;
    now = time(0);
    cout << ctime(&now);
    log->printExpInfo("WT " + to_string(serialNum));
    DataSource* dataSourceWt = new LineItemSource(lineItemPath.c_str());
    TestBench* benchWT = new WiredTigerBench(path + "/wttest" + to_string(serialNum), true, dataSourceWt, style);
//    benchWT->setBatchPara(1, 0);
    double writePercentage = 0;
    double scanPercentage = 0;
    int lastWriteOutput = 0;
    int lastScanOutput = 0;

    int currentRows = 0;
    int insertRows = 0;
    for (int i = 0; i < wtRunTurn; i++) {
        while(writePercentage < 25 * (i + 1) && (insertRows = benchWT->batchInsert(log->batchDatas, log->records, batchSize, batchSize - 1)) != 0){
            currentRows += insertRows;
            writePercentage = currentRows * 1.0 / totalRows * 100;
            if (writePercentage - lastWriteOutput > 1) {
                lastWriteOutput++;
                now = time(0);
                cout << ctime(&now) << "write " << writePercentage << "%" << endl;
            }
        }
        waitUntilNextFlush();
        while(scanPercentage < 25 * (i + 1)) {
            benchWT->batchRead(log->batchDatas, log->records, batchSize, batchSize - 1);
            scanPercentage += 1.0 / totalScan * 100;
            if (scanPercentage - lastScanOutput > 1) {
                lastScanOutput++;
                now = time(0);
                cout << ctime(&now) << "scan " << scanPercentage << "%" << endl;
            }
        }
    }
    log->writeToDisk(path + "/wttest" + to_string(serialNum) +".log");
    benchWT->close();
    delete log;
    delete dataSourceWt;

    totalScan = 10000;
    totalLength = 138;
    log = new ExpData();
    log->actualRedundantBit = 0;
    log->valueRange = valueRange;
    log->valueConsecutive = consecutive;
    log->isRandom = isRandom;
    log->use_lsm = true;
    log->rowSize = totalLength;
    log->keyField = 4;
    log->valueField = 12;
    now = time(0);
    cout << ctime(&now);
    log->printExpInfo("RocksDB " + to_string(serialNum));
    DataSource* dataSourceR = new LineItemSource(lineItemPath.c_str());
    rocksdb::Options options;
    TestBench* benchR = new RocksdbBench(path + "/rtest" + to_string(serialNum), true, options, dataSourceR, style);
    benchR->setBatchPara(1,0);
    writePercentage = 0;
    scanPercentage = 0;
    lastWriteOutput = 0;
    lastScanOutput = 0;
    insertRows = 0;
    currentRows = 0;
    for (int i = 0; i < 4; i++) {
        while(writePercentage < 25 * (i + 1) && (insertRows = benchR->batchInsert(log->batchDatas, log->records, batchSize, batchSize - 1)) != 0){
            currentRows += insertRows;
            writePercentage = currentRows * 1.0 / totalRows * 100;
            if (writePercentage - lastWriteOutput > 1) {
                lastWriteOutput++;
                now = time(0);
                cout << ctime(&now) << "write " << writePercentage << "%" << endl;
            }
        }
        waitUntilNextFlush();
        while(scanPercentage < 25 * (i + 1)) {
            benchR->batchRead(log->batchDatas, log->records, batchSize, batchSize - 1);
            scanPercentage +=  1.0 / totalScan * 100;
            if (scanPercentage - lastScanOutput > 1) {
                lastScanOutput++;
                now = time(0);
                cout << ctime(&now) << "scan " << scanPercentage << "%" << endl;
            }
        }
    }
    log->writeToDisk(path + "/rtest" + to_string(serialNum) +".log");
    benchR->close();
    delete log;
    delete dataSourceR;
}

void GenerateSampler::waitUntilNextFlush() {
    time_t now = time(0);
    cout << ctime(&now);
    tm *ltm = localtime(&now);
    int min = ltm->tm_min;

    int next = (min / 5 + 1) * 5 - min;
    cout << "sleep for " << next << " minute(s)" << endl;
    std::this_thread::sleep_for (std::chrono::seconds (next * 60 + 5));

}

/**
 * 执行wiredtiger列式存储的生成数据写入测试
 * @param path
 * @param style
 * @param serialNum
 */
void GenerateSampler::runColumnExp(string &path, TestStyle style, int serialNum) {
    bool isRandom = false;
    if (style == RAND || style == RAND_CHOPPED) {
        isRandom = true;
    }

    ColumnStoreFormat* format = initColumnFormat();
    unsigned int totalLength = format->getIntFields() * 4;
    // 随机字符串属性长度加到totalLength
    if (format->getStrFields() != 0) {
        unsigned int minLength = totalLength + (format->getStrFields() * 4);
        unsigned int maxLength = random() % 1024;
        if (maxLength < minLength + 4) {
            maxLength = minLength + 4;
        }
        totalLength = random() % (maxLength - minLength) + minLength;

    }

    unsigned int valueRange = 10 + random() % 53;
    unsigned int consecutive = 1 + random() % 4;
    // total 2GB data, at least 1M rows, and 10000 scan
    unsigned int totalRows = (1 << 31) / totalLength + 1;
    if (isRandom) {
        totalRows = totalRows * 3 / 4;
    }
    if (totalLength <= 12) {
        totalRows = totalRows / 2;
    }

    unsigned int singleScanSize = 200;
    auto* source = new ColumnStoreSource(isRandom, singleScanSize, totalRows);
    // 设置回format
    source->setFormat(format, totalLength, consecutive, valueRange);
    unsigned int batchSize = 4000;


    auto* writeLog = new ExpData();
    vector<ExpData*> readLogs;
    writeLog->valueRange = valueRange;
    writeLog->valueConsecutive = consecutive;
    writeLog->isRandom = isRandom;
    writeLog->use_column = true;
    writeLog->rowSize = totalLength;
    writeLog->intField = source->getIntFields();
    writeLog->strField = source->getStrFields();
    time_t now = time(nullptr);
    cout << ctime(&now);
    writeLog->printExpInfo("COL " + to_string(serialNum));
    writeLog->actualRedundantBit = 0;

    auto* benchWT = new WiredTigerColumnBench(path + "/ctest" + to_string(serialNum), true, source, style);
    benchWT->setBatchPara(1, 0);
    double writePercentage = 0;
    int lastWriteOutput = 0;
    unsigned int currentRows = 0;
    unsigned int insertRows = 0;

    for (int i = 0; i < 4; i++) {
        auto startTime = chrono::system_clock::now();
        while(writePercentage < 25 * (i + 1) && (insertRows =  benchWT->batchInsert(writeLog->batchDatas, writeLog->records, batchSize, batchSize - 1)) != 0){
            currentRows += insertRows;
            writePercentage = currentRows * 1.0 / totalRows * 100;
            if (writePercentage - lastWriteOutput > 1) {
                lastWriteOutput++;
                now = time(nullptr);
                cout << ctime(&now) << "write " << writePercentage << "%" << endl;
                auto endTime = chrono::system_clock::now();
                auto duration = chrono::duration_cast<chrono::minutes>(endTime - startTime);
                if (duration.count() > 25) {
                    // 超过25分钟则跳过本批剩余数据
                    currentRows = ceil(totalRows * (25.0 * ( i + 1 ) / 100));
                    break;
                }
            }
        }
#ifndef SAMPLER_DEBUG
        waitUntilNextFlush();
#endif
    }
    writeLog->writeToDisk(path + "/ctest" + to_string(serialNum) + ".log");
    benchWT->close();
    delete writeLog;
    delete source;
    delete format;
}


ColumnStoreFormat *GenerateSampler::initColumnFormat() {
    ColumnStoreFormat* format;
    int typeNo = random() % FORMAT_TYPE_SIZE;
    switch (typeNo) {
        case 0:
            format = new I1Format();
            break;
        case 1:
            format = new S1Format();
            break;
        case 2:
            format = new I2Format();
            break;
        case 3:
            format = new I1S1Format();
            break;
        case 4:
            format = new S2Format();
            break;
        case 5:
            format = new I3Format();
            break;
        case 6:
            format = new I1S2Format();
            break;
        case 7:
            format = new S3Format();
            break;
        case 8:
            format = new I1S3Format();
            break;
        case 9:
            format = new I2S3Format();
            break;
        case 10:
            format = new I2S5Format();
            break;
        case 11:
            format = new I2S8Format();
            break;
        case 12:
            format = new I5S8Format();
            break;
        case 13:
            format = new I5S12Format();
            break;
        default:
            format = new I1Format();
            cout << "ERROR in GenerateSampler::initColumnFormat(): broken code";
    }
    return format;
}

/**
 * 执行wiredtiger列式存储的生成数据读出测试
 * @param path
 */
void GenerateSampler::runColumnFCExp(string &path, int start, int end) {
    for (int serialNum = start; serialNum <= end; serialNum++) {
        int intNum = 0;
        int strNum = 0;
        getFormatFromText(serialNum, intNum, strNum);

        waitUntilNextFlush();
        WT_CONNECTION* conn;
        WT_SESSION* session;
        string folder = path + "/ctest" + to_string(serialNum);
        wiredtiger_open(folder.c_str(), nullptr, "create", &conn);
        conn->open_session(conn, nullptr, nullptr, &session);
        for (int i = 0; i < intNum; i++) {
            auto startTime = chrono::system_clock::now();
            string title = "colgroup:userinform:int_" + to_string(i);
            int size = 0;
            long resultSize = WiredTigerColumnBench::fullScan(session, title, true, size);
            auto endTime = chrono::system_clock::now();
            auto duration = chrono::duration_cast<chrono::milliseconds>(endTime - startTime);
            cout << "Table: " << serialNum << " Col:" << i << " FieldSize:" << size << " ReadRows:" << resultSize << " UseTime:" << duration.count() <<" Type:INT" << endl;

        }
        for (int i = 0; i < strNum; i++) {
            auto startTime = chrono::system_clock::now();
            string title = "colgroup:userinform:str_" + to_string(i);
            int size = 0;
            long resultSize = WiredTigerColumnBench::fullScan(session, title, false, size);
            auto endTime = chrono::system_clock::now();
            auto duration = chrono::duration_cast<chrono::milliseconds>(endTime - startTime);
            cout << "Table: " << serialNum << " Col:" << i << " FieldSize:" << size << " ReadRows:" << resultSize << " UseTime:" << duration.count() << " Type:STR" << endl;
        }
        session->close(session, nullptr);
        conn->close(conn, nullptr);
    }

}

void GenerateSampler::getFormatFromText(int serialNum, int& retIntNum, int& strFieldNum) {
    string s = "COL 1 StringField:0 IntField:3\n"
               "COL 2 StringField:0 IntField:2\n"
               "COL 3 StringField:2 IntField:1\n"
               "COL 4 StringField:8 IntField:5\n"
               "COL 5 StringField:0 IntField:3\n"
               "COL 6 StringField:1 IntField:0\n"
               "COL 7 StringField:0 IntField:2\n"
               "COL 8 StringField:2 IntField:0\n"
               "COL 9 StringField:3 IntField:1\n"
               "COL 10 StringField:2 IntField:1\n"
               "COL 11 StringField:8 IntField:2\n"
               "COL 12 StringField:1 IntField:1\n"
               "COL 13 StringField:1 IntField:1\n"
               "COL 14 StringField:8 IntField:2\n"
               "COL 15 StringField:0 IntField:2\n"
               "COL 16 StringField:2 IntField:0\n"
               "COL 17 StringField:5 IntField:2\n"
               "COL 18 StringField:2 IntField:1\n"
               "COL 19 StringField:1 IntField:1\n"
               "COL 20 StringField:1 IntField:1\n"
               "COL 21 StringField:1 IntField:1\n"
               "COL 22 StringField:3 IntField:1\n"
               "COL 23 StringField:8 IntField:2\n"
               "COL 24 StringField:0 IntField:3\n"
               "COL 25 StringField:0 IntField:2\n"
               "COL 26 StringField:3 IntField:1\n"
               "COL 27 StringField:3 IntField:0\n"
               "COL 28 StringField:5 IntField:2\n"
               "COL 29 StringField:3 IntField:2\n"
               "COL 30 StringField:8 IntField:2\n"
               "COL 31 StringField:3 IntField:1\n"
               "COL 32 StringField:0 IntField:2\n"
               "COL 33 StringField:0 IntField:2\n"
               "COL 34 StringField:2 IntField:1\n"
               "COL 35 StringField:2 IntField:0\n"
               "COL 36 StringField:1 IntField:1\n"
               "COL 37 StringField:3 IntField:0\n"
               "COL 38 StringField:12 IntField:5\n"
               "COL 39 StringField:12 IntField:5\n"
               "COL 40 StringField:2 IntField:0\n"
               "COL 41 StringField:5 IntField:2\n"
               "COL 42 StringField:3 IntField:1\n"
               "COL 43 StringField:3 IntField:1\n"
               "COL 44 StringField:1 IntField:1\n"
               "COL 45 StringField:3 IntField:1\n"
               "COL 46 StringField:2 IntField:1\n"
               "COL 47 StringField:2 IntField:0\n"
               "COL 48 StringField:12 IntField:5\n"
               "COL 49 StringField:0 IntField:1\n"
               "COL 50 StringField:3 IntField:1";
    vector<string> slist;
    boost::algorithm::split(slist, s, boost::is_any_of("\n"));
    ColumnStoreFormat* format = nullptr;
    for (int i = 0; i < slist.size(); i++) {
        vector<string> fields;
        boost::algorithm::split(fields, slist[i], boost::is_any_of(" "));
        if (stoi(fields[1]) == serialNum) {
            string strFieldstr = fields[2];
            int pos = strFieldstr.find(':');
            strFieldstr = strFieldstr.substr(pos + 1);
            string intFieldstr = fields[3];
            pos = intFieldstr.find(':');
            intFieldstr = intFieldstr.substr(pos + 1);
            retIntNum = stoi(intFieldstr);
            strFieldNum = stoi(strFieldstr);
        }
    }
}

/**
 * 执行wiredtiger列式存储的TPC数据写入测试
 * @param lineItemPath
 * @param path
 * @param style
 * @param serialNum
 */
void GenerateSampler::runTPCColumnExp(string &lineItemPath, string &path, TestStyle style, int serialNum) {
    bool isRandom = false;
    if (style == RAND || style == RAND_CHOPPED) {
        isRandom = true;
    }

    ColumnStoreFormat* format = new LineItemFormat();
    unsigned int totalLength = 128;
    unsigned int valueRange = 54;
    unsigned int consecutive = 1;

    unsigned int totalRows = 11997996;
    auto* source = new LineItemColumnSource(isRandom, lineItemPath);
    source->setFormat(format, totalLength, consecutive, valueRange);
    unsigned int batchSize = 4000;

    auto* writeLog = new ExpData();
    writeLog->valueRange = valueRange;
    writeLog->valueConsecutive = consecutive;
    writeLog->isRandom = isRandom;
    writeLog->use_column = true;
    writeLog->rowSize = totalLength;
    writeLog->intField = source->getIntFields();
    writeLog->strField = source->getStrFields();
    time_t now = time(nullptr);
    cout << ctime(&now);
    writeLog->printExpInfo("COL " + to_string(serialNum));
    writeLog->actualRedundantBit = 0;

    auto* benchWT = new WiredTigerColumnBench(path + "/ctest" + to_string(serialNum), true, source, style);
    benchWT->setBatchPara(1, 0);
    double writePercentage = 0;
    int lastWriteOutput = 0;
    unsigned int currentRows = 0;
    unsigned int insertRows = 0;

    for (int i = 0; i < 4; i++) {
        auto startTime = chrono::system_clock::now();
        while(writePercentage < 25 * (i + 1) && (insertRows =  benchWT->batchInsert(writeLog->batchDatas, writeLog->records, batchSize, batchSize - 1)) != 0){
            currentRows += insertRows;
            writePercentage = currentRows * 1.0 / totalRows * 100;
            if (writePercentage - lastWriteOutput > 1) {
                lastWriteOutput++;
                now = time(nullptr);
                cout << ctime(&now) << "write " << writePercentage << "%" << endl;
                auto endTime = chrono::system_clock::now();
                auto duration = chrono::duration_cast<chrono::minutes>(endTime - startTime);
                if (duration.count() > 25) {
                    // 超过25分钟则跳过本批剩余数据
                    currentRows = ceil(totalRows * (25.0 * ( i + 1 ) / 100));
                    break;
                }
            }
        }
#ifndef SAMPLER_DEBUG
        waitUntilNextFlush();
#endif
    }
    writeLog->writeToDisk(path + "/ctest" + to_string(serialNum) + ".log");

    benchWT->close();
    delete writeLog;
    delete source;
    delete format;
}

void GenerateSampler::runUserInformColumnExp(string &userInformPath, string &path, TestStyle style, int serialNum) {
    bool isRandom = false;
    if (style == RAND || style == RAND_CHOPPED) {
        isRandom = true;
    }

    ColumnStoreFormat* format = new UserInformFormat();
    unsigned int totalLength = 39;
    unsigned int valueRange = 100;
    unsigned int consecutive = 1;

    unsigned int totalRows = 10000;
    auto* source = new UserInformColumnSource(isRandom, userInformPath);
    source->setFormat(format, totalLength, consecutive, valueRange);
    unsigned int batchSize = 40;

    auto* writeLog = new ExpData();
    writeLog->valueRange = valueRange;
    writeLog->valueConsecutive = consecutive;
    writeLog->isRandom = isRandom;
    writeLog->use_column = true;
    writeLog->rowSize = totalLength;
    writeLog->intField = source->getIntFields();
    writeLog->strField = source->getStrFields();
    time_t now = time(nullptr);
    cout << ctime(&now);
    writeLog->printExpInfo("COL " + to_string(serialNum));
    writeLog->actualRedundantBit = 0;

    auto* benchWT = new WiredTigerColumnBench(path + "/ctest" + to_string(serialNum), true, source, style);
    benchWT->setBatchPara(1, 0);
    double writePercentage = 0;
    int lastWriteOutput = 0;
    unsigned int currentRows = 0;
    unsigned int insertRows = 0;

    for (int i = 0; i < 4; i++) {
        auto startTime = chrono::system_clock::now();
        while(writePercentage < 25 * (i + 1) && (insertRows =  benchWT->batchInsert(writeLog->batchDatas, writeLog->records, batchSize, batchSize - 1)) != 0){
            currentRows += insertRows;
            writePercentage = currentRows * 1.0 / totalRows * 100;
            if (writePercentage - lastWriteOutput > 1) {
                lastWriteOutput++;
                now = time(nullptr);
                cout << ctime(&now) << "write " << writePercentage << "%" << endl;
                auto endTime = chrono::system_clock::now();
                auto duration = chrono::duration_cast<chrono::minutes>(endTime - startTime);
                if (duration.count() > 25) {
                    // 超过25分钟则跳过本批剩余数据
                    currentRows = ceil(totalRows * (25.0 * ( i + 1 ) / 100));
                    break;
                }
            }
        }
#ifndef SAMPLER_DEBUG
        waitUntilNextFlush();
#endif
    }
    writeLog->writeToDisk(path + "/ctest" + to_string(serialNum) + ".log");

    benchWT->close();
    delete writeLog;
    delete source;
    delete format;
}

void GenerateSampler::runUserBehaviorColumnExp(string &userBehaviorPath, string &path, TestStyle style, int serialNum) {
    bool isRandom = false;
    if (style == RAND || style == RAND_CHOPPED) {
        isRandom = true;
    }

    ColumnStoreFormat* format = new UserBehaviorFormat();
    unsigned int totalLength = 49;
    unsigned int valueRange = 100;
    unsigned int consecutive = 1;

    unsigned int totalRows = 100000;
    auto* source = new UserBehaviorColumnSource(isRandom, userBehaviorPath);
    source->setFormat(format, totalLength, consecutive, valueRange);
    unsigned int batchSize = 400;

    auto* writeLog = new ExpData();
    writeLog->valueRange = valueRange;
    writeLog->valueConsecutive = consecutive;
    writeLog->isRandom = isRandom;
    writeLog->use_column = true;
    writeLog->rowSize = totalLength;
    writeLog->intField = source->getIntFields();
    writeLog->strField = source->getStrFields();
    time_t now = time(nullptr);
    cout << ctime(&now);
    writeLog->printExpInfo("COL " + to_string(serialNum));
    writeLog->actualRedundantBit = 0;

    auto* benchWT = new WiredTigerColumnBench(path + "/ctest" + to_string(serialNum), true, source, style);
    benchWT->setBatchPara(1, 0);
    double writePercentage = 0;
    int lastWriteOutput = 0;
    unsigned int currentRows = 0;
    unsigned int insertRows = 0;

    for (int i = 0; i < 4; i++) {
        auto startTime = chrono::system_clock::now();
        while(writePercentage < 25 * (i + 1) && (insertRows =  benchWT->batchInsert(writeLog->batchDatas, writeLog->records, batchSize, batchSize - 1)) != 0){
            currentRows += insertRows;
            writePercentage = currentRows * 1.0 / totalRows * 100;
            if (writePercentage - lastWriteOutput > 1) {
                lastWriteOutput++;
                now = time(nullptr);
                cout << ctime(&now) << "write " << writePercentage << "%" << endl;
                auto endTime = chrono::system_clock::now();
                auto duration = chrono::duration_cast<chrono::minutes>(endTime - startTime);
                if (duration.count() > 25) {
                    // 超过25分钟则跳过本批剩余数据
                    currentRows = ceil(totalRows * (25.0 * ( i + 1 ) / 100));
                    break;
                }
            }
        }
#ifndef SAMPLER_DEBUG
        waitUntilNextFlush();
#endif
    }
    writeLog->writeToDisk(path + "/ctest" + to_string(serialNum) + ".log");

    benchWT->close();
    delete writeLog;
    delete source;
    delete format;
}
