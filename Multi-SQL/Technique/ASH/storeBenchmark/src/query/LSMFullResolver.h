//
// Created by ironwei on 2020/5/16.
//

#ifndef STOREBENCHMARK_LSMFULLRESOLVER_H
#define STOREBENCHMARK_LSMFULLRESOLVER_H


#include <rocksdb/db.h>
#include "BaseResolver.h"

class LSMFullResolver: public BaseResolver {
public:
    LSMFullResolver(string& tpcTablePath, string& lineItemDataPath, string& workDir);
    void loadData(unsigned long preLoad, unsigned long actualLoad) override;
    void readQuery1(int query) override;
    void readQuery2(int query) override;
    void execQ3(int query) override;
    void execQ5(int query) override;
    ~LSMFullResolver() override;
private:
    rocksdb::DB* _ins = nullptr;
    rocksdb::WriteOptions _writeOptions;
    string _keyFormat;
};


#endif //STOREBENCHMARK_LSMFULLRESOLVER_H
