//
// Created by ironwei on 2020/3/23.
//

#include <fstream>
#include <cmath>
#include <chrono>
#include <sstream>
#include <thread>
#include <bitset>
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "boost/filesystem/path.hpp"
#include "boost/filesystem/operations.hpp"
#include "boost/algorithm/string/split.hpp"
#include "boost/algorithm/string.hpp"
#include "LineItemQueryResolver.h"
#include "WiredTigerBench.h"
#include "query/Q3.h"
#include "query/Q5.h"

#define USE_ROOT
//#define SKIP_WAIT

LineItemQueryResolver::LineItemQueryResolver() {
    stringstream sb;
    sb << "%0" << ORDERKEY_LEN << "d%0" << PARTKEY_LEN << "d%0"
       << SUPPKEY_LEN << "d%0" << LINENUMBER_LEN <<"d";
    _keyFormat = sb.str();
}

void LineItemQueryResolver::initTPCTable(string &tpcDataPath, string &tpcTablePath, bool overwrite) {
    boost::filesystem::path path(tpcTablePath);
    bool needCreateTable = false;
    if (boost::filesystem::exists(path)) {
        if (overwrite) {
            system(("rm -rf " + tpcTablePath).c_str());
            boost::filesystem::create_directory(path);
            needCreateTable = true;
        }
    } else {
        boost::filesystem::create_directory(path);
        needCreateTable = true;
    }


    if (needCreateTable) {
        wiredtiger_open(tpcTablePath.c_str(), nullptr, "create", &_COConn);
        _COConn->open_session(_COConn, nullptr, nullptr, &_COSession);
        const int BUFFER_LEN = 512;
        char buffer[BUFFER_LEN] = {};

        /**
         * create tables: customer, orders, supplier, nation, region
         */
        // create customer(CUSTKEY) index(SEGMENT)
        WiredTigerBench::error_check(_COSession->create(_COSession, "table:customer",
                                                      "key_format=I,value_format=SSI15sI10sS,"
                                                      "columns=(CUSTKEY,NAME,ADDRESS,NATIONKEY,PHONE,ACCTBAL,MKTSEGMENT,COMMENT)"),
                                     "LineItemQueryResolver::initTPCTable():0");
        WiredTigerBench::error_check(
                _COSession->create(_COSession, "index:customer:segment", "columns=(MKTSEGMENT)"),
                "LineItemQueryResolver::initTPCTable():1");
        WiredTigerBench::error_check(
                _COSession->create(_COSession, "index:customer:nation", "columns=(NATIONKEY)"),
                "LineItemQueryResolver::initTPCTable():1");
        // create orders(ORDERKEY) index(CUSTKEY) COLGROUP(main, other)
        WiredTigerBench::error_check(_COSession->create(_COSession, "table:orders",
                                                      "key_format=I,value_format=I1sI10s15s15sIS,"
                                                      "columns=(ORDERKEY,CUSTKEY,ORDERSTATUS,TOTALPRICE,ORDERDATE,ORDERPRIORITY,CLERK,SHIPPRIORITY,COMMENT),"
                                                      "colgroups=(main,other)"),
                                     "LineItemQueryResolver::initTPCTable():2");
        WiredTigerBench::error_check(
                _COSession->create(_COSession, "colgroup:orders:main", "columns=(CUSTKEY,ORDERDATE,SHIPPRIORITY)"),
                "LineItemQueryResolver::initTPCTable():3");
        WiredTigerBench::error_check(
                _COSession->create(_COSession, "colgroup:orders:other", "columns=(ORDERSTATUS,TOTALPRICE,ORDERPRIORITY,CLERK,COMMENT)"),
                "LineItemQueryResolver::initTPCTable():4");
        WiredTigerBench::error_check(
                _COSession->create(_COSession, "index:orders:custkey", "columns=(CUSTKEY)"),
                "LineItemQueryResolver::initTPCTable():5");
        // create supplier(SUPPKEY) index(NATIONKEY)
        WiredTigerBench::error_check(_COSession->create(_COSession, "table:supplier", "key_format=I,value_format=25sSI15sIS,"
                                                        "columns=(SUPPKEY,NAME,ADDRESS,NATIONKEY,PHONE,ACCTBAL,COMMENT)"),
                                     "LineItemQueryResolver::initTPCTable():6");
        WiredTigerBench::error_check(_COSession->create(_COSession, "index:supplier:nation", "columns=(NATIONKEY)"),
                "LineItemQueryResolver::initTPCTable():7");
        // create nation(NATIONKEY) index(regionKEY)
        WiredTigerBench::error_check(_COSession->create(_COSession, "table:nation", "key_format=I,value_format=25sIS,"
                                                        "columns=(NATIONKEY,NAME,REGIONKEY,COMMENT)"),
                                     "LineItemQueryResolver::initTPCTable():8");
        WiredTigerBench::error_check(_COSession->create(_COSession, "index:nation:region", "columns=(REGIONKEY)"),
                "LineItemQueryResolver::initTPCTable():9");
        // create region
        WiredTigerBench::error_check(_COSession->create(_COSession, "table:region", "key_format=I,value_format=25sS,"
                                                        "columns=(REGIONKEY,NAME,COMMENT)"),
                                     "LineItemQueryResolver::initTPCTable():10");

        // write customer to db
        string _path = tpcDataPath + "/customer.tbl";
        std::fstream _fstream1;
        _fstream1.open(_path);
        vector<string> lines;
        while (_fstream1.getline(buffer, BUFFER_LEN)) {
            if (strlen(buffer) > 0) {
                std::string line(buffer);
                lines.push_back(line);
            }
        }
        _fstream1.close();
        WiredTigerBench::error_check(_COSession->open_cursor(_COSession, "table:customer", nullptr, "append", &_cursor),
                                     "LineItemQueryResolver::initTPCTable():11");
        for (auto & line : lines) {
            vector<string> fields;
            boost::split(fields, line, boost::is_any_of( "|" ) );
            int custKey = stoi(fields[0]);
            const char* name = fields[1].c_str();
            const char* address = fields[2].c_str();
            int nationKey = stoi(fields[3]);
            char phone[15];
            memset(phone, 0, 15);
            memcpy(phone, fields[4].c_str(), fields[4].size());
            int acctbal = (int)round(stod(fields[5]) * 100);
            char mktsegment[10];
            memset(mktsegment, 0, 10);
            memcpy(mktsegment, fields[6].c_str(), fields[6].size());
            const char* comment = fields[7].c_str();
            _cursor->set_key(_cursor, custKey);
            _cursor->set_value(_cursor, name, address, nationKey, phone, acctbal, mktsegment, comment);
            WiredTigerBench::error_check(_cursor->insert(_cursor), "LineItemQueryResolver::initTPCTable():12");
        }
        WiredTigerBench::error_check(_cursor->close(_cursor), "LineItemQueryResolver::initTPCTable():13");

        // write orders to db
        _path = tpcDataPath + "/orders.tbl";
        std::fstream _fstream2;
        _fstream2.open(_path);
        lines.clear();
        while (_fstream2.getline(buffer, BUFFER_LEN)) {
            if (strlen(buffer) > 0) {
                std::string line(buffer);
                lines.push_back(line);
            }
        }
        _fstream2.close();
        WiredTigerBench::error_check(_COSession->open_cursor(_COSession, "table:orders", nullptr, "append", &_cursor),
                                     "LineItemQueryResolver::initTPCTable():14");
        for (auto & line : lines) {
            vector<string> fields;
            boost::split(fields, line, boost::is_any_of( "|" ) );
            int orderKey = stoi(fields[0]);
            int custKey = stoi(fields[1]);
            char orderStatus[1] {0};
            orderStatus[0] = fields[2].c_str()[0];
            int totalPrice = (int)round(stod(fields[3]) * 100);
            char orderDate[10];
            memset(orderDate, 0, 10);
            memcpy(orderDate, fields[4].c_str(), 10);
            char orderPrior[15];
            memset(orderPrior, 0, 15);
            memcpy(orderPrior, fields[5].c_str(), fields[5].size());
            char clerk[15];
            memset(clerk, 0, 15);
            memcpy(clerk, fields[6].c_str(), fields[6].size());
            int shipPrior = stoi(fields[7]);
            const char* comment = fields[8].c_str();
            _cursor->set_key(_cursor, orderKey);
            _cursor->set_value(_cursor, custKey, orderStatus, totalPrice, orderDate, orderPrior, clerk, shipPrior, comment);
            WiredTigerBench::error_check(_cursor->insert(_cursor), "LineItemQueryResolver::initTPCTable():15");
        }
        WiredTigerBench::error_check(_cursor->close(_cursor), "LineItemQueryResolver::initTPCTable():16");

        // write supplier to db
        _path = tpcDataPath + "/supplier.tbl";
        std::fstream _fstream3;
        _fstream3.open(_path);
        lines.clear();
        while (_fstream3.getline(buffer, BUFFER_LEN)) {
            if (strlen(buffer) > 0) {
                std::string line(buffer);
                lines.push_back(line);
            }
        }
        _fstream3.close();
        WiredTigerBench::error_check(_COSession->open_cursor(_COSession, "table:supplier", nullptr, "append", &_cursor),
                                     "LineItemQueryResolver::initTPCTable():17");
        for (auto & line : lines) {
            vector<string> fields;
            boost::split(fields, line, boost::is_any_of( "|" ) );
            int suppKey = stoi(fields[0]);
            char name[25];
            memset(name, 0, 25);
            memcpy(name, fields[1].c_str(), 25);
            const char* address = fields[2].c_str();
            int nationKey = stoi(fields[3]);
            char phone[15];
            memset(phone, 0, 15);
            memcpy(phone, fields[4].c_str(), 15);
            int acctbal = (int)round(stod(fields[5]) * 100);;
            const char* comment = fields[6].c_str();
            _cursor->set_key(_cursor, suppKey);
            _cursor->set_value(_cursor, name, address, nationKey, phone, acctbal, comment);
            WiredTigerBench::error_check(_cursor->insert(_cursor), "LineItemQueryResolver::initTPCTable():18");
        }
        WiredTigerBench::error_check(_cursor->close(_cursor), "LineItemQueryResolver::initTPCTable():19");

        // write supplier to db
        _path = tpcDataPath + "/nation.tbl";
        std::fstream _fstream4;
        _fstream4.open(_path);
        lines.clear();
        while (_fstream4.getline(buffer, BUFFER_LEN)) {
            if (strlen(buffer) > 0) {
                std::string line(buffer);
                lines.push_back(line);
            }
        }
        _fstream4.close();
        WiredTigerBench::error_check(_COSession->open_cursor(_COSession, "table:nation", nullptr, "append", &_cursor),
                                     "LineItemQueryResolver::initTPCTable():20");
        for (auto & line : lines) {
            vector<string> fields;
            boost::split(fields, line, boost::is_any_of( "|" ) );
            int nationKey = stoi(fields[0]);
            char name[25];
            memset(name, 0, 25);
            memcpy(name, fields[1].c_str(), 25);
            int regionKey = stoi(fields[2]);
            const char* comment = fields[3].c_str();
            _cursor->set_key(_cursor, nationKey);
            _cursor->set_value(_cursor, name, regionKey, comment);
            WiredTigerBench::error_check(_cursor->insert(_cursor), "LineItemQueryResolver::initTPCTable():21");
        }
        WiredTigerBench::error_check(_cursor->close(_cursor), "LineItemQueryResolver::initTPCTable():22");

        // write region to db
        _path = tpcDataPath + "/region.tbl";
        std::fstream _fstream5;
        _fstream5.open(_path);
        lines.clear();
        while (_fstream5.getline(buffer, BUFFER_LEN)) {
            if (strlen(buffer) > 0) {
                std::string line(buffer);
                lines.push_back(line);
            }
        }
        _fstream5.close();
        WiredTigerBench::error_check(_COSession->open_cursor(_COSession, "table:region", nullptr, "append", &_cursor),
                                     "LineItemQueryResolver::initTPCTable():23");
        for (auto & line : lines) {
            vector<string> fields;
            boost::split(fields, line, boost::is_any_of( "|" ) );
            int regionKey = stoi(fields[0]);
            char name[25];
            memset(name, 0, 25);
            memcpy(name, fields[1].c_str(), 25);
            const char* comment = fields[2].c_str();
            _cursor->set_key(_cursor, regionKey);
            _cursor->set_value(_cursor, name, comment);
            WiredTigerBench::error_check(_cursor->insert(_cursor), "LineItemQueryResolver::initTPCTable():24");
        }
        WiredTigerBench::error_check(_cursor->close(_cursor), "LineItemQueryResolver::initTPCTable():25");
    } else {
        wiredtiger_open(tpcTablePath.c_str(), nullptr, nullptr, &_COConn);
        _COConn->open_session(_COConn, nullptr, nullptr, &_COSession);
    }
}

void LineItemQueryResolver::loadLineItem(string &LIPath, string &testTablePath, bool overwrite, int clockBeginPercent = 0, int loadEndPercent = 100) {
    boost::filesystem::path path(testTablePath);
    lineItemWTPath = testTablePath + "/wiredtiger";
    lineItemRocksPath = testTablePath + "/rocksdb";
    boost::filesystem::path wtPath(lineItemWTPath);
    boost::filesystem::path rPath(lineItemRocksPath);
    bool needCreateTable = false;
    if (boost::filesystem::exists(path) && boost::filesystem::exists(wtPath) && boost::filesystem::exists(rPath)) {
        if (overwrite) {
            system(("rm -rf " + testTablePath).c_str());
            boost::filesystem::create_directory(path);
            boost::filesystem::create_directory(wtPath);
            boost::filesystem::create_directory(rPath);
            needCreateTable = true;
        }
    } else {
        boost::filesystem::create_directory(path);
        boost::filesystem::create_directory(wtPath);
        boost::filesystem::create_directory(rPath);
        needCreateTable = true;
    }

    if (needCreateTable) {
        wiredtiger_open(lineItemWTPath.c_str(), nullptr, "create", &_LIConn);
        _LIConn->open_session(_LIConn, nullptr, nullptr, &_LISession);
        const int BUFFER_LEN = 256;
        char buffer[BUFFER_LEN] = {};
        std::fstream _fstream;
        _fstream.open(LIPath);
        vector<string> lines;
        while (_fstream.getline(buffer, BUFFER_LEN)) {
            if (strlen(buffer) > 0) {
                std::string line(buffer);
                lines.push_back(line);
            }
        }
        _fstream.close();

        string columnNames = "ORDERKEY,PARTKEY,SUPPKEY,LINENUMBER,QUANTITY,EXTENDPRICE,DISCOUNT,TAX,RETURNFLAG,"
                            "LINESTATUS,SHIPDATE,COMMITDATE,RECEIPTDATE,SHIPINSTRUCT,SHIPMODE,COMMENT";
        string colgroups = columnNames;
        boost::algorithm::to_lower(colgroups);
        vector<string> columnList;
        vector<string> colgroupList;
        boost::algorithm::split(columnList, columnNames, boost::is_any_of( "," ));
        boost::algorithm::split(colgroupList, colgroups, boost::is_any_of( "," ));

        // create lineitem
        string config = "key_format=IIIH,value_format=HSSS1s1sSSSSSS,columns=(" + columnNames + ")";
        WiredTigerBench::error_check(_LISession->create(_LISession, "table:lineitem", config.c_str()),
                                     "LineItemQueryResolver::loadLineItem create lineitem");
        // create lineitemcol
        config = "key_format=r,value_format=IIIHHSSS1s1sSSSSSS,columns=(ID," + columnNames + "),colgroups=(" + colgroups + ")";
        WiredTigerBench::error_check(_LISession->create(_LISession, "table:lineitemcol", config.c_str()),
                                     "LineItemQueryResolver::loadLineItem create lineitemcol");
        for (int i = 0; i < columnList.size(); i++) {
            string title = "colgroup:lineitemcol:" + colgroupList[i];
            config = "columns=(" + columnList[i] + ")";
            WiredTigerBench::error_check(_LISession->create(_LISession, title.c_str(), config.c_str()),
                                         "LineItemQueryResolver::loadLineItem create lineitemcol colgroup");
        }

        // write data to lineitem
        WiredTigerBench::error_check(_LISession->open_cursor(_LISession, "table:lineitem", nullptr, "append", &_cursor),
                                     "LineItemQueryResolver::loadLineItem 1");
        bool clockup = false;
        auto startTime = chrono::system_clock::now();
        for (int i = 0; i < lines.size(); i++) {
            if (!clockup && i * 100 / lines.size() >= clockBeginPercent) {
                startTime = chrono::system_clock::now();
                clockup = true;
            }
            if (i * 100 / lines.size() >= loadEndPercent) {
                break;
            }

            LineItemStruct* item = LineItemSource::parseLineItemStruct(lines[i]);

            _cursor->set_key(_cursor, item->orderKey, item->partKey, item->suppKey, item->lineNumber);
            _cursor->set_value(_cursor, item->quantity, item->extendPrice, item->discount, item->tax, item->returnFlag,
                    item->lineStatus, item->shipDate, item->commitDate, item->receiptDate, item->shipInstruct,
                    item->shipMode, item->comment);
            WiredTigerBench::error_check(_cursor->insert(_cursor), "LineItemQueryResolver::loadLineItem 2");
            delete item;
#ifdef DEBUG_WY
            if (i % 10000 == 0) {
                cout << "lineitemcol" << i * 100.0 / lines.size() << "%" << endl;
            }
#endif

        }
        WiredTigerBench::error_check(_cursor->close(_cursor), "LineItemQueryResolver::loadLineItem 3");
        auto endTime = chrono::system_clock::now();
        auto duration = chrono::duration_cast<chrono::milliseconds>(endTime - startTime);
        cout << "insert lineitem wt Time:" << duration.count() << "ms" << endl;

        // write data to lineitemcol
        clockup = false;
        WiredTigerBench::error_check(_LISession->open_cursor(_LISession, "table:lineitemcol", nullptr, "append", &_cursor),
                                     "LineItemQueryResolver::loadLineItem 4");
        for (int i = 0; i < lines.size(); i++) {
            if (!clockup && i * 100 / lines.size() >= clockBeginPercent) {
                startTime = chrono::system_clock::now();
                clockup = true;
            }
            if (i * 100 / lines.size() >= loadEndPercent) {
                break;
            }
            LineItemStruct* item = LineItemSource::parseLineItemStruct(lines[i]);

            _cursor->set_value(_cursor, item->orderKey, item->partKey, item->suppKey, item->lineNumber, item->quantity,
                    item->extendPrice, item->discount, item->tax, item->returnFlag, item->lineStatus, item->shipDate,
                    item->commitDate, item->receiptDate, item->shipInstruct, item->shipMode, item->comment);
            WiredTigerBench::error_check(_cursor->insert(_cursor), "LineItemQueryResolver::loadLineItem 5");
            delete item;
#ifdef DEBUG_WY
            if (i % 10000 == 0) {
                cout << "lineitemcol" << i * 100.0 / lines.size() << "%" << endl;
            }
#endif
        }
        WiredTigerBench::error_check(_cursor->close(_cursor), "LineItemQueryResolver::loadLineItem 6");
        endTime = chrono::system_clock::now();
        duration = chrono::duration_cast<chrono::milliseconds>(endTime - startTime);
        cout << "insert lineitemcol wt Time:" << duration.count() << "ms" << endl;

        rocksdb::Options options;
        // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
        options.IncreaseParallelism();
        options.OptimizeLevelStyleCompaction();
        // create the DB if it's not already present
        options.create_if_missing = true;
        rocksdb::Status s = rocksdb::DB::Open(options, lineItemRocksPath, &_ins);
        assert(s.ok());

        clockup = false;
        for (int i = 0; i < lines.size(); i++) {
            if (!clockup && i * 100 / lines.size() >= clockBeginPercent) {
                startTime = chrono::system_clock::now();
                clockup = true;
            }
            if (i * 100 / lines.size() >= loadEndPercent) {
                break;
            }
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
#ifdef DEBUG_WY
            if (i % 10000 == 0) {
                cout << "lineitemcol" << i * 100.0 / lines.size() << "%" << endl;
            }
#endif
        }
        endTime = chrono::system_clock::now();
        duration = chrono::duration_cast<chrono::milliseconds>(endTime - startTime);
        cout << "insert lineitem lsm Time:" << duration.count() << "ms" << endl;
#ifdef DEBUG_WY
        cout << "done!" << endl;
#endif
    } else {
        wiredtiger_open(lineItemWTPath.c_str(), nullptr, nullptr, &_LIConn);
        _LIConn->open_session(_LIConn, nullptr, nullptr, &_LISession);

        rocksdb::Options options;
        // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
        options.IncreaseParallelism();
        options.OptimizeLevelStyleCompaction();
        // create the DB if it's not already present
        options.create_if_missing = true;
        rocksdb::Status s = rocksdb::DB::Open(options, lineItemRocksPath, &_ins);
        assert(s.ok());
    }
}

void LineItemQueryResolver::waitUntilNextFlush() {
#ifdef SKIP_WAIT
    cout << "skip waitUntilNextFlush" << endl;
    return;
#endif
#ifdef USE_ROOT
    cout << "clear cache..." << endl;
    system("sh /home/weiyan/clearCache.sh");
    std::this_thread::sleep_for (std::chrono::seconds (10));
    return;
#endif
    time_t now = time(0);
    cout << ctime(&now);
    tm *ltm = localtime(&now);
    int min = ltm->tm_min;

    int next = (min / 5 + 1) * 5 - min;
    cout << "sleep for " << next << " minute(s)" << endl;
    std::this_thread::sleep_for (std::chrono::seconds (next * 60 + 5));
}

void LineItemQueryResolver::batchExecQ3(vector<string> &segments, vector<int> &dayOfMonths) {
    Q3 q3(_COSession, _LISession, _ins, _keyFormat);
    q3.batchExecQ3(segments, dayOfMonths);

}

void LineItemQueryResolver::batchExecQ5(vector<string> &region, vector<int> &year) {
    Q5 q5(_COSession, _LISession, _ins, _keyFormat);
    q5.batchExecQ5(region, year);
}

LineItemQueryResolver::~LineItemQueryResolver() {
    if (_COSession != nullptr) {
        _COSession->close(_COSession, nullptr);
    }
    if (_COConn != nullptr) {
        _COConn->close(_COConn, nullptr);
    }
    if (_LISession != nullptr) {
        _LISession->close(_LISession, nullptr);
    }
    if (_LIConn != nullptr) {
        _LIConn->close(_LIConn, nullptr);
    }
    _COSession = nullptr;
    _COConn = nullptr;
    _LISession = nullptr;
    _LIConn = nullptr;

    if (_ins != nullptr) {
        _ins->Close();
    }
    _ins = nullptr;
}

