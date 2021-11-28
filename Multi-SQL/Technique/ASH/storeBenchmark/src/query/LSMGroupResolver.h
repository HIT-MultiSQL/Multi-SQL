//
// Created by ironwei on 2020/5/16.
//

#ifndef STOREBENCHMARK_LSMGROUPRESOLVER_H
#define STOREBENCHMARK_LSMGROUPRESOLVER_H


#include "BaseResolver.h"
#include "rocksdb/db.h"

class LSMGroupResolver: public BaseResolver {
public:
    LSMGroupResolver(string& tpcTablePath, string& lineItemDataPath, string& workDir);
    void loadData(unsigned long preLoad, unsigned long actualLoad) override;
    void readQuery1(int query) override;
    void readQuery2(int query) override;
    void execQ3(int query) override;
    void execQ5(int query) override;
    ~LSMGroupResolver() override;
private:
    rocksdb::DB* _ins = nullptr;
    rocksdb::ColumnFamilyHandle* mainGroup = nullptr;
    rocksdb::ColumnFamilyHandle* otherGroup = nullptr;
    rocksdb::WriteOptions _writeOptions;
    string _keyFormat;
};


#endif //STOREBENCHMARK_LSMGROUPRESOLVER_H
