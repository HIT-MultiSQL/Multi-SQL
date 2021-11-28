//
// Created by yanhao on 2020/7/31.
//

#ifndef STOREBENCHMARK_USERBEHAVIOR_H
#define STOREBENCHMARK_USERBEHAVIOR_H

#include <string>
#include "dataDesc.h"

using namespace std;
class UserBehavior {
public:
    const int &getId() const;
    void setId(int id);
    const int &getUid() const;
    void setUid(int uid);
    const std::string &getLogTime() const;
    void setLogTime(const std::string &logTime);
    const std::string &getIp() const;
    void setIp(const std::string ip);
    const std::string &getDevice() const;
    void setDevice(const std::string device);
private:
    int _id = 0;
    int _uid = 0;
    std::string _logTime;
    std::string _ip;
    std::string _device;
};

struct UserBehaviorStruct {
    int id = 0;
    int uid = 0;
    char* logTime = nullptr;
    char* ip = nullptr;
    char* device = nullptr;
    ~UserBehaviorStruct() {
        delete[] logTime;
        delete[] ip;
        delete[] device;
    }
};
#endif //STOREBENCHMARK_USERBEHAVIOR_H
