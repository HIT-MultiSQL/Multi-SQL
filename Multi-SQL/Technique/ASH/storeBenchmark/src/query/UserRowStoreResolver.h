//
// Created by yanhao on 2020/8/2.
//

#ifndef STOREBENCHMARK_USERROWSTORERESOLVER_H
#define STOREBENCHMARK_USERROWSTORERESOLVER_H

#include "UserBaseResolver.h"

class UserRowStoreResolver: public UserBaseResolver {
public:
    UserRowStoreResolver(string& informTablePath, string& behaviorTablePath,
            string& userInformPath, string& userBehaviorPath, string& workDir);
    void loadData(unsigned long preLoad1, unsigned long actualLoad1, unsigned long preLoad2, unsigned long actualLoad2) override;
    void createIndex() override;
    void execQ1() override;
    void execQ2() override;
    void execQ3() override;
    void execQ4() override;
    void execQ5() override;
    ~UserRowStoreResolver() override;
private:
    WT_CONNECTION *_Conn;
    WT_SESSION *_Session;
    bool print = true;
};
#endif //STOREBENCHMARK_USERROWSTORERESOLVER_H
