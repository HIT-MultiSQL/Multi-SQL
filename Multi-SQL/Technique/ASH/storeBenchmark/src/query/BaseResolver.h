//
// Created by ironwei on 2020/5/16.
//

#ifndef STOREBENCHMARK_BASERESOLVER_H
#define STOREBENCHMARK_BASERESOLVER_H


#include <wiredtiger.h>
#include <string>

using namespace std;
class BaseResolver {
public:
    explicit BaseResolver(string& tpcTablePath, string& lineItemPath, string& workDir);
    virtual void loadData(unsigned long preLoad, unsigned long actualLoad) = 0;
    virtual void readQuery1(int query) = 0;
    virtual void readQuery2(int query) = 0;
    virtual void execQ3(int query) = 0;
    virtual void execQ5(int query) = 0;
    virtual ~BaseResolver();
protected:
    string _lineItemPath;
    string _workDir;
    WT_CONNECTION *_COConn = nullptr;
    WT_SESSION *_COSession = nullptr;
    unsigned long _totalRows;
};


#endif //STOREBENCHMARK_BASERESOLVER_H
