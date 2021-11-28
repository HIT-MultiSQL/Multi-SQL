//
// Created by yanhao on 2020/8/10.
//

#ifndef STOREBENCHMARK_USERLSMFULLRESOLVER_H
#define STOREBENCHMARK_USERLSMFULLRESOLVER_H

#include <rocksdb/db.h>
#include "UserBaseResolver.h"

class UserLSMFullResolver: public UserBaseResolver {
public:
    UserLSMFullResolver(string &informTablePath, string &behaviorTablePath, string &userInformPath,
                        string &userBehaviorPath, string &workDir);
    void loadData(unsigned long preLoad1, unsigned long actualLoad1, unsigned long preLoad2, unsigned long actualLoad2) override;
    void createIndex() override ;
    void execQ1() override;
    void execQ2() override;
    void execQ3() override;
    void execQ4() override;
    void execQ5() override;
    ~UserLSMFullResolver() override;
private:
    rocksdb::DB* _inform_db = nullptr;
    rocksdb::DB* _behavior_db = nullptr;
    rocksdb::WriteOptions _inform_writeOptions;
    rocksdb::WriteOptions _behavior_writeOptions;
    string _inform_keyFormat;
    string _behavior_keyFormat;
    bool print = true;
};
#endif //STOREBENCHMARK_USERLSMFULLRESOLVER_H
