//
// Created by ironwei on 2020/5/16.
//

#include "RowStoreResolver.h"
#include "../WiredTigerBench.h"
#include "Q3.h"
#include "../LineItemQueryResolver.h"
#include "Q5.h"
#include <cstring>
#include <fstream>
#include <iostream>
#include <chrono>
#include <vector>
#include <boost/algorithm/string.hpp>

RowStoreResolver::RowStoreResolver(string &tpcTablePath, string &lineItemDataPath, string &workDir) : BaseResolver(
        tpcTablePath, lineItemDataPath, workDir) {
    wiredtiger_open(workDir.c_str(), nullptr, "create", &_LIConn);
    _LIConn->open_session(_LIConn, nullptr, nullptr, &_LISession);

    //参数配置
    string columnNames = "ORDERKEY,PARTKEY,SUPPKEY,LINENUMBER,QUANTITY,EXTENDPRICE,DISCOUNT,TAX,RETURNFLAG,"
                         "LINESTATUS,SHIPDATE,COMMITDATE,RECEIPTDATE,SHIPINSTRUCT,SHIPMODE,COMMENT";
    string config = "key_format=IIIH,value_format=HSSS1s1sSSSSSS,columns=(" + columnNames + ")";

    //创建表lineitem
    WiredTigerBench::error_check(_LISession->create(_LISession, "table:lineitem", config.c_str()),
                                 "RowStoreResolver::loadData create lineitem");
    cout << "execute row store test..." << endl;
}

void RowStoreResolver::loadData(unsigned long preLoad, unsigned long actualLoad) {
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
    WiredTigerBench::error_check(_LISession->open_cursor(_LISession, "table:lineitem", nullptr, "append", &_cursor),
                                 "RowStoreResolver::loadData 1");
    cout << "preLoading " << preLoad << " rows" << endl;
    for (unsigned long i = 0; i < preLoad; i++) {
        LineItemStruct* item = LineItemSource::parseLineItemStruct(lines[i]);

        _cursor->set_key(_cursor, item->orderKey, item->partKey, item->suppKey, item->lineNumber);
        _cursor->set_value(_cursor, item->quantity, item->extendPrice, item->discount, item->tax, item->returnFlag,
                           item->lineStatus, item->shipDate, item->commitDate, item->receiptDate, item->shipInstruct,
                           item->shipMode, item->comment);
        WiredTigerBench::error_check(_cursor->insert(_cursor), "RowStoreResolver::loadData 2");
        delete item;
    }
    cout << "insert for " << actualLoad << " rows" << endl;
    auto startTime = chrono::system_clock::now();
    for (unsigned long i = preLoad; i < preLoad + actualLoad; i++) {
        LineItemStruct* item = LineItemSource::parseLineItemStruct(lines[i]);

        _cursor->set_key(_cursor, item->orderKey, item->partKey, item->suppKey, item->lineNumber);
        _cursor->set_value(_cursor, item->quantity, item->extendPrice, item->discount, item->tax, item->returnFlag,
                           item->lineStatus, item->shipDate, item->commitDate, item->receiptDate, item->shipInstruct,
                           item->shipMode, item->comment);
        WiredTigerBench::error_check(_cursor->insert(_cursor), "RowStoreResolver::loadData 3");
        delete item;
    }
    auto endTime = chrono::system_clock::now();
    auto duration = chrono::duration_cast<chrono::seconds>(endTime - startTime);
    cout << "***insert lineitem time:" << duration.count() << "s" << endl;
    WiredTigerBench::error_check(_cursor->close(_cursor), "RowStoreResolver::loadData 4");
    _totalRows = preLoad + actualLoad;
}

RowStoreResolver::~RowStoreResolver() {
    if (_LISession != nullptr) {
        _LISession->close(_LISession, nullptr);
    }
    _LISession = nullptr;
    if (_LIConn != nullptr) {
        _LIConn->close(_LIConn, nullptr);
    }
    _LIConn = nullptr;
}

void RowStoreResolver::readQuery1(int query) {
    int resultSize = 0;
    int exact = 0;
    int ret = 0;
    int orderKey;
    int suppKey;
    int partKey;
    int lineNumber;
    uint16_t quantity;
    const char* extendPrice;
    const char* discount;
    const char* tax;
    const char* returnFlag;
    const char* lineStatus;
    const char* shipDate;
    const char* commitDate;
    const char* receiptDate;
    const char* shipInstruct;
    const char* shipMode;
    const char* comment;
    WT_CURSOR* _cursor = nullptr;
    auto startTime = chrono::system_clock::now();
    for (int q = 0; q < query; q++) {
        //随机生成一条记录，并寻找相等或相近的记录
        uint32_t startRowOrderKey = random() % _totalRows;
        uint32_t endRowOrderKey = startRowOrderKey + 4;
        WiredTigerBench::error_check(_LISession->open_cursor(_LISession, "table:lineitem", nullptr, nullptr, &_cursor),
                "RowStoreResolver::readQuery1 1");
        _cursor->set_key(_cursor, startRowOrderKey, 0, 0, 0);
        WiredTigerBench::error_check(_cursor->search_near(_cursor, &exact), "RowStoreResolver::readQuery1 2");
        _cursor->get_key(_cursor, &orderKey, &partKey, &suppKey, &lineNumber);
        if (orderKey < startRowOrderKey) {
            while ((ret = _cursor->next(_cursor)) == 0) {
                _cursor->get_key(_cursor, &orderKey, &partKey, &suppKey, &lineNumber);
                if (orderKey >= startRowOrderKey) {
                    if (orderKey < endRowOrderKey) {
                        _cursor->get_value(_cursor, &quantity, &extendPrice, &discount, &tax, &returnFlag, &lineStatus, &shipDate, &commitDate,
                                           &receiptDate, &shipInstruct, &shipMode, &comment);
                        resultSize++;
                    }
                    break;
                }
            }
        }

        //遍历满足 orderKey < endRowOrderKey的记录
        while ((ret = _cursor->next(_cursor)) == 0) {
            _cursor->get_key(_cursor, &orderKey, &partKey, &suppKey, &lineNumber);
            if (orderKey < endRowOrderKey) {
                _cursor->get_value(_cursor, &quantity, &extendPrice, &discount, &tax, &returnFlag, &lineStatus, &shipDate, &commitDate,
                                   &receiptDate, &shipInstruct, &shipMode, &comment);
                resultSize ++;
            } else {
                break;
            }
        }
        if (ret != WT_NOTFOUND && ret != 0) {
            WiredTigerBench::error_check(ret, "RowStoreResolver::readQuery1 3");
        }
        WiredTigerBench::error_check(_cursor->close(_cursor), "RowStoreResolver::readQuery1 4");
    }
    auto endTime = chrono::system_clock::now();
    auto duration = chrono::duration_cast<chrono::seconds>(endTime - startTime);
    cout << "*** execute single read: read " << resultSize << " rows, time " << duration.count() << "s" << endl;
}

void RowStoreResolver::readQuery2(int query) {
    int resultSize = 0;
    int exact = 0;
    int ret = 0;
    int orderKey;
    int suppKey;
    int partKey;
    int lineNumber;
    uint16_t quantity;
    const char* extendPrice;
    const char* discount;
    const char* returnFlag;
    const char* lineStatus;
    const char* shipDate;
    const char* commitDate;
    const char* receiptDate;
    WT_CURSOR* _cursor = nullptr;
    auto startTime = chrono::system_clock::now();
    for (int q = 0; q < query; q++) {
        uint32_t startRowOrderKey = random() % _totalRows;
        uint32_t endRowOrderKey = startRowOrderKey + 4;
        WiredTigerBench::error_check(_LISession->open_cursor(_LISession, "table:lineitem(QUANTITY,EXTENDPRICE,"
             "DISCOUNT,RETURNFLAG,LINESTATUS,SHIPDATE,COMMITDATE,RECEIPTDATE)", nullptr, nullptr, &_cursor), "RowStoreResolver::readQuery2 1");
        _cursor->set_key(_cursor, startRowOrderKey, 0, 0, 0);
        WiredTigerBench::error_check(_cursor->search_near(_cursor, &exact), "RowStoreResolver::readQuery2 2");
        _cursor->get_key(_cursor, &orderKey, &partKey, &suppKey, &lineNumber);
        if (orderKey < startRowOrderKey) {
            while ((ret = _cursor->next(_cursor)) == 0) {
                _cursor->get_key(_cursor, &orderKey, &partKey, &suppKey, &lineNumber);
                if (orderKey >= startRowOrderKey) {
                    if (orderKey < endRowOrderKey) {
                        _cursor->get_value(_cursor, &quantity, &extendPrice, &discount, &returnFlag, &lineStatus, &shipDate, &commitDate,
                                           &receiptDate);
                        resultSize++;
                    }
                    break;
                }
            }
        }
        while ((ret = _cursor->next(_cursor)) == 0) {
            _cursor->get_key(_cursor, &orderKey, &partKey, &suppKey, &lineNumber);
            if (orderKey < endRowOrderKey) {
                _cursor->get_value(_cursor, &quantity, &extendPrice, &discount, &returnFlag, &lineStatus, &shipDate, &commitDate,
                                   &receiptDate);
                resultSize ++;
            } else {
                break;
            }
        }
        if (ret != WT_NOTFOUND && ret != 0) {
            WiredTigerBench::error_check(ret, "RowStoreResolver::readQuery2 3");
        }
        WiredTigerBench::error_check(_cursor->close(_cursor), "RowStoreResolver::readQuery1 4");
    }
    auto endTime = chrono::system_clock::now();
    auto duration = chrono::duration_cast<chrono::seconds>(endTime - startTime);
    cout << "*** execute single read: read " << resultSize << " rows, time " << duration.count() << "s" << endl;
}

void RowStoreResolver::execQ3(int query) {
    string dummy;
    Q3 solver(_COSession, _LISession, nullptr, dummy);
    solver.EXEC_BT = true;

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

void RowStoreResolver::execQ5(int query) {
    string dummy;
    Q5 solver(_COSession, _LISession, nullptr, dummy);
    solver.EXEC_BT = true;

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
