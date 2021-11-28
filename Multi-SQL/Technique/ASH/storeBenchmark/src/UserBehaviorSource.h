//
// Created by yanhao on 2020/7/31.
//

#ifndef STOREBENCHMARK_USERBEHAVIORSOURCE_H
#define STOREBENCHMARK_USERBEHAVIORSOURCE_H
#include <string>
#include <vector>
#include "wiredtiger.h"
#include "wiredtiger_ext.h"
#include "dataDesc.h"
#include "DataSource.h"
#include "UserBehavior.h"

class UserBehaviorSource : public DataSource {
public:
    explicit UserBehaviorSource(const char* filePath);
    string getWTKeyFormat() const final { string s = "I"; return s; }
    string getWTValueFormat() const final { string s = "ISSS"; return s; }
    string getWTColumnNames() const final { string s = "id,uid,log_time,ip,device"; return s;};
    int getKeyFieldsNum() const final { return 1; }
    int getValueFieldsNum() const final { return 4; }
    string getNextKeyString() final;
    string initLeadKeyString(long leadKey) const final ;
    long getCurrentLeadKey() const final { return _currentLeadKey;};
    long wtSetNextKey(WT_CURSOR* cursor) final;
    string getNextValueString() final;
    void wtSetNextValue(WT_CURSOR* cursor) final;
    long wtGetCursorLeadKey(WT_CURSOR* cursor) const final;
    long parseLeadKeyFromString(string keyString) const final;
    void wtInitLeadKeyCursor(WT_CURSOR* cursor, long leadKey) const final;
    bool hasNext() const final;
    void close() final;
    virtual ~UserBehaviorSource(){
        delete _currentItem;
        delete _currentItemStruct;
    }

    static UserBehaviorStruct* parseUserBehaviorStruct(const std::string &line);
private:
    UserBehavior* next();
    UserBehaviorStruct* nextItemStruct();
    static UserBehavior* parseUserBehavior(const std::string &line);
    UserBehavior* _currentItem = nullptr;
    UserBehaviorStruct* _currentItemStruct = nullptr;
    long _currentLeadKey = 0;
    const char* _path;
    std::vector<std::string> _lines;
    int _pos = 0;
    static const int BUFFER_LEN = 256;
    char _buffer[BUFFER_LEN] = {};
    std::string _keyFormat;

};
#endif //STOREBENCHMARK_USERBEHAVIORSOURCE_H
