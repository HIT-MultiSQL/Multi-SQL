//
// Created by ironwei on 2020/5/16.
//

#include <fstream>
#include <vector>
#include <cstring>
#include <iostream>
#include "ColumnStoreResolver.h"
#include "../WiredTigerBench.h"
#include "Q3.h"
#include "../LineItemQueryResolver.h"
#include "Q5.h"
#include <chrono>
#include <boost/algorithm/string.hpp>

ColumnStoreResolver::ColumnStoreResolver(string &tpcTablePath, string &lineItemDataPath, string &workDir):
    BaseResolver(tpcTablePath, lineItemDataPath, workDir) {
    wiredtiger_open(workDir.c_str(), nullptr, "create", &_LIConn);
    _LIConn->open_session(_LIConn, nullptr, nullptr, &_LISession);

    string columnNames = "ORDERKEY,PARTKEY,SUPPKEY,LINENUMBER,QUANTITY,EXTENDPRICE,DISCOUNT,TAX,RETURNFLAG,"
                         "LINESTATUS,SHIPDATE,COMMITDATE,RECEIPTDATE,SHIPINSTRUCT,SHIPMODE,COMMENT";
    string colgroups = columnNames;
    boost::algorithm::to_lower(colgroups);
    vector<string> columnList;
    vector<string> colgroupList;
    boost::algorithm::split(columnList, columnNames, boost::is_any_of( "," ));
    boost::algorithm::split(colgroupList, colgroups, boost::is_any_of( "," ));

    // create lineitemcol
    string config = "key_format=r,value_format=IIIHHSSS1s1sSSSSSS,columns=(ID," + columnNames + "),colgroups=(" + colgroups + ")";
    WiredTigerBench::error_check(_LISession->create(_LISession, "table:lineitemcol", config.c_str()),
                                 "ColumnStoreResolver::loadData create lineitemcol");
    for (int i = 0; i < columnList.size(); i++) {
        string title = "colgroup:lineitemcol:" + colgroupList[i];
        config = "columns=(" + columnList[i] + ")";
        WiredTigerBench::error_check(_LISession->create(_LISession, title.c_str(), config.c_str()),
                                     "ColumnStoreResolver::loadData create lineitemcol colgroup");
    }
    cout << "execute column store test..." << endl;
}

void ColumnStoreResolver::loadData(unsigned long preLoad, unsigned long actualLoad) {
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


    WT_CURSOR* _cursor = nullptr;
    WiredTigerBench::error_check(_LISession->open_cursor(_LISession, "table:lineitemcol", nullptr, "append", &_cursor),
                                 "ColumnStoreResolver::loadData 1");
    cout << "preLoading " << preLoad << " rows" << endl;
    for (unsigned long i = 0; i < preLoad; i++) {
        LineItemStruct* item = LineItemSource::parseLineItemStruct(lines[i]);
        _cursor->set_value(_cursor, item->orderKey, item->partKey, item->suppKey, item->lineNumber, item->quantity,
                           item->extendPrice, item->discount, item->tax, item->returnFlag, item->lineStatus, item->shipDate,
                           item->commitDate, item->receiptDate, item->shipInstruct, item->shipMode, item->comment);
        WiredTigerBench::error_check(_cursor->insert(_cursor), "ColumnStoreResolver::loadData 2");
        delete item;
    }
    cout << "insert for " << actualLoad << " rows" << endl;
    auto startTime = chrono::system_clock::now();
    for (unsigned long i = preLoad; i < preLoad + actualLoad; i++) {
        LineItemStruct* item = LineItemSource::parseLineItemStruct(lines[i]);
        _cursor->set_value(_cursor, item->orderKey, item->partKey, item->suppKey, item->lineNumber, item->quantity,
                           item->extendPrice, item->discount, item->tax, item->returnFlag, item->lineStatus, item->shipDate,
                           item->commitDate, item->receiptDate, item->shipInstruct, item->shipMode, item->comment);
        WiredTigerBench::error_check(_cursor->insert(_cursor), "ColumnStoreResolver::loadData 3");
        delete item;
    }
    auto endTime = chrono::system_clock::now();
    auto duration = chrono::duration_cast<chrono::seconds>(endTime - startTime);
    cout << "***insert lineitem time:" << duration.count() << "s" << endl;
    WiredTigerBench::error_check(_cursor->close(_cursor), "ColumnStoreResolver::loadData 4");
    _totalRows = preLoad + actualLoad;
}

ColumnStoreResolver::~ColumnStoreResolver() {
    if (_LISession != nullptr) {
        _LISession->close(_LISession, nullptr);
    }
    _LISession = nullptr;
    if (_LIConn != nullptr) {
        _LIConn->close(_LIConn, nullptr);
    }
    _LIConn = nullptr;

}

void ColumnStoreResolver::execQ3(int query) {
    string dummy;
    Q3 solver(_COSession, _LISession, nullptr, dummy);
    solver.EXEC_COL_SCAN = true;

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

void ColumnStoreResolver::execQ5(int query) {
    string dummy;
    Q5 solver(_COSession, _LISession, nullptr, dummy);
    solver.EXEC_COL_SCAN = true;

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
