//
// Created by ironwei on 2020/5/16.
//

#include <fstream>
#include <iostream>

#include "LSMFullResolver.h"
#include "../LineItem.h"
#include "../DataSource.h"
#include "Q3.h"
#include "../LineItemQueryResolver.h"
#include "Q5.h"
#include <sstream>

LSMFullResolver::LSMFullResolver(string &tpcTablePath, string &lineItemDataPath, string &workDir):
    BaseResolver(tpcTablePath, lineItemDataPath, workDir) {
    stringstream sb;
    sb << "%0" << ORDERKEY_LEN << "d%0" << PARTKEY_LEN << "d%0"
       << SUPPKEY_LEN << "d%0" << LINENUMBER_LEN <<"d";
    _keyFormat = sb.str();

    rocksdb::Options options;
    options.IncreaseParallelism();
    options.OptimizeLevelStyleCompaction();
    options.create_if_missing = true;
    rocksdb::Status s = rocksdb::DB::Open(options, _workDir, &_ins);
    assert(s.ok());
    cout << "execute lsm (single colgroup) test..." << endl;
}

void LSMFullResolver::loadData(unsigned long preLoad, unsigned long actualLoad) {
    const int BUFFER_LEN = 256;
    char buffer[BUFFER_LEN] = {};
    std::fstream _fstream;
    _fstream.open(_lineItemPath);
    vector<string> lines;
    while (_fstream.getline(buffer, BUFFER_LEN)) {
        if (strlen(buffer) > 0) {
            std::string line(buffer);
            lines.push_back(line);
        }
    }
    _fstream.close();

    if (preLoad > lines.size()) {
        preLoad = lines.size();
    }
    if (actualLoad > lines.size() - preLoad) {
        actualLoad = lines.size() - preLoad;
    }

    cout << "preLoading " << preLoad << " rows" << endl;
    for (unsigned long i = 0; i < preLoad; i++) {
        LineItemStruct* item = LineItemSource::parseLineItemStruct(lines[i]);
        char leadKeyBuf[LINEITEM_KEY_LEN];
        memset(leadKeyBuf, 0, LINEITEM_KEY_LEN);
        sprintf(leadKeyBuf, _keyFormat.data(), item->orderKey, item->partKey, item->suppKey, item->lineNumber);

        string key(leadKeyBuf);
        stringstream value;
        value << item->quantity << "|" << item->extendPrice << "|" << item->discount << "|" << item->tax << "|";
        value << item->returnFlag << "|" << item->lineStatus << "|" << item->shipDate << "|" << item->receiptDate << "|";
        value << item->shipInstruct << "|" << item->shipMode << "|" << item->comment;
        _ins->Put(_writeOptions, key, value.str());
        delete item;
    }
    cout << "insert for " << actualLoad << " rows" << endl;
    auto startTime = chrono::system_clock::now();
    for (unsigned long i = preLoad; i < preLoad + actualLoad; i++) {
        LineItemStruct* item = LineItemSource::parseLineItemStruct(lines[i]);
        char leadKeyBuf[LINEITEM_KEY_LEN];
        memset(leadKeyBuf, 0, LINEITEM_KEY_LEN);
        sprintf(leadKeyBuf, _keyFormat.data(), item->orderKey, item->partKey, item->suppKey, item->lineNumber);

        string key(leadKeyBuf);
        stringstream value;
        value << item->quantity << "|" << item->extendPrice << "|" << item->discount << "|" << item->tax << "|";
        value << item->returnFlag << "|" << item->lineStatus << "|" << item->shipDate << "|" << item->receiptDate << "|";
        value << item->shipInstruct << "|" << item->shipMode << "|" << item->comment;
        _ins->Put(_writeOptions, key, value.str());
        delete item;
    }
    auto endTime = chrono::system_clock::now();
    auto duration = chrono::duration_cast<chrono::seconds>(endTime - startTime);
    cout << "***insert lineitem time:" << duration.count() << "s" << endl;
    _totalRows = preLoad + actualLoad;
}

LSMFullResolver::~LSMFullResolver() {
    if (_ins != nullptr) {
        _ins->Close();
    }
    _ins = nullptr;
}

void LSMFullResolver::readQuery1(int query) {
    unsigned int resultSize = 0;
    auto startTime = chrono::system_clock::now();
    for (int q = 0; q < query; q++) {
        int orderKey = (int)(random() % _totalRows);

        char leadKeyBuf[LINEITEM_KEY_LEN];
        sprintf(leadKeyBuf, _keyFormat.data(), orderKey, 0, 0, 0);
        string start = string(leadKeyBuf);
        sprintf(leadKeyBuf, _keyFormat.data(), orderKey + 4, 0, 0, 0);
        string end = string(leadKeyBuf);
        rocksdb::Iterator* it = _ins->NewIterator(rocksdb::ReadOptions());
        it->Seek(start);
        if (it->Valid() && it->key().ToString() < end) {
            string keyString = it->key().ToString();
            string valueString = it->value().ToString();
            it->Next();
            resultSize++;
            while (it->Valid() && it->key().ToString() < end) {
                keyString = it->key().ToString();
                valueString = it->value().ToString();
                it->Next();
                resultSize++;
            }
        }
        assert(it->status().ok()); // Check for any errors found during the scan
        delete it;
    }
    auto endTime = chrono::system_clock::now();
    auto duration = chrono::duration_cast<chrono::seconds>(endTime - startTime);
    cout << "*** execute single read: read " << resultSize << " rows, time " << duration.count() << "s" << endl;
}

void LSMFullResolver::readQuery2(int query) {
    readQuery1(query);
}

void LSMFullResolver::execQ3(int query) {
    Q3 solver(_COSession, nullptr, _ins, _keyFormat);
    solver.EXEC_LSM = true;

    cout << "exec " << query << " Q3" << endl;
    long totalTimeMilliSec = 0;
    vector<string> segments;
    segments.emplace_back("AUTOMOBILE");
    segments.emplace_back("BUILDING");
    segments.emplace_back("FURNITURE");
    segments.emplace_back("MACHINERY");
    segments.emplace_back("HOUSEHOLD");
    while (query > 0) {
        for (int i = 0; i < 5; i++) {
            auto startTime = chrono::system_clock::now();
            query--;
            vector<string> para1;
            para1.emplace_back(segments[i]);
            vector<int> para2;
            para2.emplace_back(random() % 31 + 1);
            solver.batchExecQ3(para1, para2);
            auto endTime = chrono::system_clock::now();
            auto duration = chrono::duration_cast<chrono::milliseconds>(endTime - startTime);
            totalTimeMilliSec += duration.count();
            LineItemQueryResolver::waitUntilNextFlush();
            if (query <= 0) {
                break;
            }
        }
    }
    cout << "*** execute q3 time " << totalTimeMilliSec / 1000 << "s" << endl;
}

void LSMFullResolver::execQ5(int query) {
    Q5 solver(_COSession, nullptr, _ins, _keyFormat);
    solver.EXEC_LSM = true;

    cout << "exec " << query << " Q5" << endl;
    long totalTimeMilliSec = 0;
    vector<string> region;
    region.emplace_back("AFRICA");
    region.emplace_back("AMERICA");
    region.emplace_back("ASIA");
    region.emplace_back("EUROPE");
    region.emplace_back("MIDDLE EAST");
    while (query > 0) {
        for (int i = 0; i < 5; i++) {
            auto startTime = chrono::system_clock::now();
            query--;
            vector<string> para1;
            para1.emplace_back(region[i]);
            vector<int> para2;
            para2.emplace_back(random() % 5 + 1993);
            solver.batchExecQ5(para1, para2);
            auto endTime = chrono::system_clock::now();
            auto duration = chrono::duration_cast<chrono::milliseconds>(endTime - startTime);
            totalTimeMilliSec += duration.count();
            LineItemQueryResolver::waitUntilNextFlush();
            if (query <= 0) {
                break;
            }
        }
    }
    cout << "*** execute q5 time " << totalTimeMilliSec / 1000 << "s" << endl;
}
