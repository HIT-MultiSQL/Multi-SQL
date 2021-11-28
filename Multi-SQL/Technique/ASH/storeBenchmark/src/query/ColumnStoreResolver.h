//
// Created by ironwei on 2020/5/16.
//

#ifndef STOREBENCHMARK_COLUMNSTORERESOLVER_H
#define STOREBENCHMARK_COLUMNSTORERESOLVER_H


#include "BaseResolver.h"

class ColumnStoreResolver: public BaseResolver {
public:
    ColumnStoreResolver(string& tpcTablePath, string& lineItemDataPath, string& workDir);
    void loadData(unsigned long preLoad, unsigned long actualLoad) override;
    void readQuery1(int query) override {};
    void readQuery2(int query) override {};
    void execQ3(int query) override;
    void execQ5(int query) override;
    ~ColumnStoreResolver() override;

private:
    WT_SESSION* _LISession;
    WT_CONNECTION* _LIConn;
};


#endif //STOREBENCHMARK_COLUMNSTORERESOLVER_H
