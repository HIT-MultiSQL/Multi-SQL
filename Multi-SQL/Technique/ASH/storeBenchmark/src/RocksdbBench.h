//
// Created by iron on 2019/11/16.
//

#ifndef STOREBENCHMARK_ROCKSDBBENCH_H
#define STOREBENCHMARK_ROCKSDBBENCH_H

#include <rocksdb/db.h>
#include <set>
#include "BenchmarkConfig.h"
#include "PageCacheStat.h"
#include "TestBench.h"

class RocksdbBench : public TestBench {
public:
    explicit RocksdbBench(
            const std::string& path, 
            bool overwrite,
            rocksdb::Options& options,
            DataSource* dataSource,
            TestStyle style);
    rocksdb::DB* getInstance();
    int insert(int rows, FixSizeSample<int>& samples) override;
    int scan(unsigned int minOrderKey, unsigned int maxOrderKey, int scanSize, int& retMinOK, int& retMaxOK) override;
    PageCacheStat* getPageCacheStat() override;
    void getFileMeta(int &outL1, int &outL2, int &outMaxLevel, long& totalSize, long& totalEntries) override;
    ~RocksdbBench();
private:
    rocksdb::DB* _ins;
    rocksdb::WriteOptions _writeOptions;
    std::string _path;
    std::string _pcCommand;
    bool noFileExist = true;
};
#endif //STOREBENCHMARK_ROCKSDBBENCH_H
