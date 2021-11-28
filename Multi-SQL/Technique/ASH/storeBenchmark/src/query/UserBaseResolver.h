//
// Created by yanhao on 2020/8/2.
//

#ifndef STOREBENCHMARK_USERBASERESOLVER_H
#define STOREBENCHMARK_USERBASERESOLVER_H
#include <wiredtiger.h>
#include <string>

using namespace std;
class UserBaseResolver {
public:
    explicit UserBaseResolver(string& informTablePath, string& behaviorTablePath,
            string& userInformPath, string& userBehaviorPath,  string& workDir);
    virtual void loadData(unsigned long preLoad1, unsigned long actualLoad1, unsigned long preLoad2, unsigned long actualLoad2) = 0;
    virtual void createIndex() = 0;
    virtual void execQ1() = 0;
    virtual void execQ2() = 0;
    virtual void execQ3() = 0;
    virtual void execQ4() = 0;
    virtual void execQ5() = 0;
    virtual ~UserBaseResolver();
protected:
    string _userInformPath;
    string _userBehaviorPath;
    string _workDir;
    WT_CONNECTION *_INFORMCOConn = nullptr;
    WT_SESSION *_INFORMCOSession = nullptr;
    WT_CONNECTION *_BEHAVIORCOConn = nullptr;
    WT_SESSION *_BEHAVIORCOSession = nullptr;
    unsigned long _totalRows;
};
#endif //STOREBENCHMARK_USERBASERESOLVER_H
