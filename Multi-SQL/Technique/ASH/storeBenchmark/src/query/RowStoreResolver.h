//
// Created by ironwei on 2020/5/16.
//

#ifndef STOREBENCHMARK_ROWSTORERESOLVER_H
#define STOREBENCHMARK_ROWSTORERESOLVER_H


#include "BaseResolver.h"

class RowStoreResolver: public BaseResolver {
public:
    RowStoreResolver(string& tpcTablePath, string& lineItemDataPath, string& workDir);
    void loadData(unsigned long preLoad, unsigned long actualLoad) override;
    void readQuery1(int query) override;
    void readQuery2(int query) override;
    void execQ3(int query) override;
    void execQ5(int query) override;
    ~RowStoreResolver() override;
private:
    WT_SESSION* _LISession;
    WT_CONNECTION* _LIConn;
};


#endif //STOREBENCHMARK_ROWSTORERESOLVER_H
