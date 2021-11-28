//
// Created by yanhao on 2020/7/30.
//

#ifndef STOREBENCHMARK_USERINFORMSOURCE_H
#define STOREBENCHMARK_USERINFORMSOURCE_H
#include <string>
#include <vector>
#include "wiredtiger.h"
#include "wiredtiger_ext.h"
#include "dataDesc.h"
#include "DataSource.h"
#include "UserInform.h"

class UserInformSource : public DataSource {
public:
    explicit UserInformSource(const char* filePath);
    string getWTKeyFormat() const final { string s = "I"; return s; }
    string getWTValueFormat() const final { string s = "SI1sSS"; return s; }
    string getWTColumnNames() const final { string s = "id,name,age,gender,occupation,register_date"; return s;};
    int getKeyFieldsNum() const final { return 1; }
    int getValueFieldsNum() const final { return 5; }
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
    virtual ~UserInformSource(){
        delete _currentItem;
        delete _currentItemStruct;
    }

    static UserInformStruct* parseUserInformStruct(const std::string &line);
private:
    UserInform* next();
    UserInformStruct* nextItemStruct();
    static UserInform* parseUserInform(const std::string &line);
    UserInform* _currentItem = nullptr;
    UserInformStruct* _currentItemStruct = nullptr;
    long _currentLeadKey = 0;
    const char* _path;
    std::vector<std::string> _lines;
    int _pos = 0;
    static const int BUFFER_LEN = 256;
    char _buffer[BUFFER_LEN] = {};
    std::string _keyFormat;

};

#endif //STOREBENCHMARK_USERINFORMSOURCE_H
