//
// Created by ironwei on 2020/5/9.
//

#ifndef STOREBENCHMARK_Q3_H
#define STOREBENCHMARK_Q3_H

#include <string>
#include <vector>
#include "wiredtiger.h"
#include "wiredtiger_ext.h"
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

//#define DEBUG_WY
using namespace std;
class Q3 {
public:
    explicit Q3(WT_SESSION* pCOSession, WT_SESSION* pLISession, rocksdb::DB* pIns, string& keyFormat) {
        this->_COSession = pCOSession;
        this->_LISession = pLISession;
        this->_ins = pIns;
        this->_keyFormat = keyFormat;
    }
    void batchExecQ3(vector<string>& segments, vector<int>& dayOfMonths);
    void setColumnFamilyHandle(rocksdb::ColumnFamilyHandle* mainGroup, rocksdb::ColumnFamilyHandle* otherGroup);
    bool EXEC_BT = false;
    bool EXEC_COL_SCAN = false;
    bool EXEC_LSM = false;
    bool EXEC_LSM_GROUP = false;
private:
    void prepareQ3(string segment, int dayOfMonth, vector<int>& orderKeys, vector<string>& orderDates,
            vector<int>& shipPriors, vector<int>& sortPos);
    WT_SESSION *_COSession = nullptr;
    WT_SESSION *_LISession = nullptr;
    rocksdb::DB* _ins = nullptr;
    string _keyFormat;
    rocksdb::ColumnFamilyHandle* _mainGroup = nullptr;
    rocksdb::ColumnFamilyHandle* _otherGroup = nullptr;
};

struct Q3Result {
    Q3Result(int orderKey, double revenue, string& orderDate, int shipPriority) {
        this->orderKey = orderKey;
        this->revenue = revenue;
        this->orderDate = orderDate;
        this->shipPriority = shipPriority;
    }
    int orderKey;
    double revenue;
    string orderDate;
    int shipPriority;
    bool operator <(Q3Result other) const  {
        return other.revenue < revenue;
    }
};


#endif //STOREBENCHMARK_Q3_H
