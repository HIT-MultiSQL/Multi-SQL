//
// Created by ironwei on 2020/3/23.
//

#ifndef STOREBENCHMARK_LINEITEMQUERYRESOLVER_H
#define STOREBENCHMARK_LINEITEMQUERYRESOLVER_H

#include <vector>
#include <string>
#include "wiredtiger.h"
#include "wiredtiger_ext.h"

using namespace std;
class LineItemQueryResolver {
public:
    LineItemQueryResolver();
    void initTPCTable(string& tpcDataPath, string& tpcTablePath, bool overwrite);
    void loadLineItem(string& LIPath, string& testTablePath, bool overwrite, int clockBeginPercent, int loadEndPercent);
    void batchExecQ3(vector<string>& segments, vector<int>& dayOfMonths);
    void batchExecQ5(vector<string> &region, vector<int> &year);
    const char* c_segments[5]{"AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD"};
    static void waitUntilNextFlush();
    ~LineItemQueryResolver();
private:
    string lineItemWTPath;
    string lineItemRocksPath;
    string _keyFormat;
    WT_CONNECTION *_COConn = nullptr;
    WT_SESSION *_COSession = nullptr;
    WT_CURSOR * _cursor = nullptr;
    WT_CONNECTION *_LIConn = nullptr;
    WT_SESSION *_LISession = nullptr;
    rocksdb::DB* _ins = nullptr;
    rocksdb::WriteOptions _writeOptions;
};

#endif //STOREBENCHMARK_LINEITEMQUERYRESOLVER_H
