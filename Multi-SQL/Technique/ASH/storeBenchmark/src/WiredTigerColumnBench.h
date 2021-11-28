//
// Created by ironwei on 2020/3/18.
//

#ifndef STOREBENCHMARK_WIREDTIGERCOLUMNBENCH_H
#define STOREBENCHMARK_WIREDTIGERCOLUMNBENCH_H

#include "TestBench.h"
#include "ColumnStoreSource.h"

class WiredTigerColumnBench : public TestBench{
public:
    explicit WiredTigerColumnBench(const string& folder,
                                   bool overwrite,
                                   ColumnStoreSource* dataSource,
                                   TestStyle style);
    void startInsert();
    void endInsert();
    int scan(unsigned int minOrderKey, unsigned int maxOrderKey, int scanSize, int& retMinOK, int& retMaxOK) override;
    int insert(int rows, FixSizeSample<int>& samples) override ;
    PageCacheStat* getPageCacheStat() override;
    void close() override;
    static long fullScan(WT_SESSION* session, string& _targetColumnName, bool isInt, int& fieldSize);
protected:
    const char * _tableName;
    const char * _colgroupPrefix;
    WT_CONNECTION *_conn;
    WT_SESSION *_session;
    WT_CURSOR * _cursor;
    std::string _pcCommand;
};


#endif //STOREBENCHMARK_WIREDTIGERCOLUMNBENCH_H
