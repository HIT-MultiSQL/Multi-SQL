//
// Created by iron on 2019/12/3.
//

#ifndef STOREBENCHMARK_WIREDTIGERBENCH_H
#define STOREBENCHMARK_WIREDTIGERBENCH_H

#include "wiredtiger.h"
#include "wiredtiger_ext.h"
#include "BenchmarkConfig.h"
#include "PageCacheStat.h"
#include "TestBench.h"

class WiredTigerBench : public TestBench{
public:
    explicit WiredTigerBench(
            const string& folder,
            bool overwrite,
            DataSource* dataSource,
            TestStyle style);
    void startInsert();
    void endInsert();
    int insert(int rows, FixSizeSample<int>& samples) override ;
    void close() override;
    int scan(unsigned int minOrderKey, unsigned int maxOrderKey, int scanSize, int& retMinOK, int& retMaxOK) override;
    PageCacheStat* getPageCacheStat() override;
    static void error_check(int error, const char* message);
    static void printError(int errorNo, const char* message);
private:
    const char * _tableName;
    WT_CONNECTION *_conn;
    WT_SESSION *_session;
    WT_CURSOR * _cursor;
    std::string _pcCommand;
};


#endif //STOREBENCHMARK_WIREDTIGERBENCH_H
