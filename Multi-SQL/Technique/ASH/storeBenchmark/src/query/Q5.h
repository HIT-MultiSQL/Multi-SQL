//
// Created by ironwei on 2020/5/9.
//

#ifndef STOREBENCHMARK_Q5_H
#define STOREBENCHMARK_Q5_H

#include <string>
#include <vector>
#include <map>
#include "wiredtiger.h"
#include "wiredtiger_ext.h"
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

using namespace std;
class Q5 {
public:
    explicit Q5(WT_SESSION* pCOSession, WT_SESSION* pLISession, rocksdb::DB* pIns, string& keyFormat) {
        this->_COSession = pCOSession;
        this->_LISession = pLISession;
        this->_ins = pIns;
        this->_keyFormat = keyFormat;
    }
    void batchExecQ5(vector<string> &region, vector<int> &year);
    void setColumnFamilyHandle(rocksdb::ColumnFamilyHandle* mainGroup, rocksdb::ColumnFamilyHandle* otherGroup);
    bool EXEC_BT = false;
    bool EXEC_COL_SCAN = false;
    bool EXEC_LSM = false;
    bool EXEC_LSM_GROUP = false;
private:
    void prepareQ5(string region, int year, vector<pair<int, string>>& countryName, vector<pair<int, int>>& suppNation, vector<pair<int, int>>& orderNation);
    void printResult(map<int, double>& revenue, vector<pair<int, string>>& countryName);
    WT_SESSION *_COSession = nullptr;
    WT_SESSION *_LISession = nullptr;
    rocksdb::DB* _ins = nullptr;
    string _keyFormat;
    rocksdb::ColumnFamilyHandle* _mainGroup = nullptr;
    rocksdb::ColumnFamilyHandle* _otherGroup = nullptr;
};


#endif //STOREBENCHMARK_Q5_H
