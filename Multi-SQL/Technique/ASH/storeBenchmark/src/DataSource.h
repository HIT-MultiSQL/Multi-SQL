//
// Created by iron on 2020/1/12.
//

#ifndef STOREBENCHMARK_DATASOURCE_H
#define STOREBENCHMARK_DATASOURCE_H

#include <string>
#include <vector>
#include "wiredtiger.h"
#include "wiredtiger_ext.h"
#include "LineItem.h"
#include "dataDesc.h"
using namespace std;
class DataSource {
public:
    virtual string getWTKeyFormat() const = 0;
    virtual string getWTValueFormat() const = 0;
    virtual string getWTColumnNames() const = 0;
    // 获取下一个key的string形式
    virtual string getNextKeyString() = 0;
    // 根据leadkey初始化key的string形式, 用于设置scan的起止leadkey
    virtual string initLeadKeyString(long leadKey) const = 0;
    // 在getNextKeyString后调用，可以获取string中的leadkey。相当于perseLeadKeyFromString(getNextKeyString())
    virtual long getCurrentLeadKey() const = 0;
    // 设置下一个key
    virtual long wtSetNextKey(WT_CURSOR* cursor) = 0;
    // 获取下一个value的string形式
    virtual string getNextValueString() = 0;
    // 设置下一个value
    virtual void wtSetNextValue(WT_CURSOR* cursor) = 0;
    // 获取当前cursor对应的leadkey
    virtual long wtGetCursorLeadKey(WT_CURSOR* cursor) const = 0;
    // 从key的string中获取leadkey。
    virtual long parseLeadKeyFromString(string keyString) const = 0;
    // 根据leadkey初始化cursor，用于设置scan的起止leadkey
    virtual void wtInitLeadKeyCursor(WT_CURSOR* cursor, long leadKey) const = 0;
    virtual int getKeyFieldsNum() const = 0;
    virtual int getValueFieldsNum() const = 0;
    virtual bool hasNext() const = 0;
    virtual void close() = 0;
    virtual ~DataSource() = default;
};

class LineItemReadOnlySource : public DataSource {
public:
    explicit LineItemReadOnlySource();
    string getWTKeyFormat() const final { string s = "IIIH"; return s; }
    string getWTValueFormat() const final { string s = "HSSS1s1sSSSSSS"; return s; }
    string getWTColumnNames() const final { string s = "ORDERKEY,PARTKEY,SUPPKEY,LINENUMBER,QUANTITY,"
                                                 "EXTENDPRICE,DISCOUNT,TAX,RETURNFLAG,LINESTATUS,SHIPDATE,"
                                                 "COMMITDATE,RECEIPTDATE,SHIPINSTRUCT,SHIPMODE,COMMENT"; return s;};
    int getKeyFieldsNum() const final { return 4; }
    int getValueFieldsNum() const final { return 12; }
    string getNextKeyString() final { return std::string();};
    string initLeadKeyString(long leadKey) const final ;
    long getCurrentLeadKey() const final { return 0;};
    long wtSetNextKey(WT_CURSOR* cursor) final { return 0;};
    string getNextValueString() final { return std::string();};
    void wtSetNextValue(WT_CURSOR* cursor) final {};
    long wtGetCursorLeadKey(WT_CURSOR* cursor) const final;
    long parseLeadKeyFromString(string keyString) const final;
    void wtInitLeadKeyCursor(WT_CURSOR* cursor, long leadKey) const final;
    bool hasNext() const final { return false; };
    void close() final {};
private:
    std::string _keyFormat;
};

class LineItemSource : public DataSource {
public:
    explicit LineItemSource(const char* filePath);
    string getWTKeyFormat() const final { string s = "IIIH"; return s; }
    string getWTValueFormat() const final { string s = "HSSS1s1sSSSSSS"; return s; }
    string getWTColumnNames() const final { string s = "ORDERKEY,PARTKEY,SUPPKEY,LINENUMBER,QUANTITY,"
                                                 "EXTENDPRICE,DISCOUNT,TAX,RETURNFLAG,LINESTATUS,SHIPDATE,"
                                                 "COMMITDATE,RECEIPTDATE,SHIPINSTRUCT,SHIPMODE,COMMENT"; return s;};
    int getKeyFieldsNum() const final { return 4; }
    int getValueFieldsNum() const final { return 12; }
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
    virtual ~LineItemSource(){
        delete _currentItem;
        delete _currentItemStruct;
    }

    static LineItemStruct* parseLineItemStruct(const std::string &line);
private:
    LineItem* next();
    LineItemStruct* nextItemStruct();
    static LineItem* parseLineItem(const std::string &line);
    LineItem* _currentItem = nullptr;
    LineItemStruct* _currentItemStruct = nullptr;
    int _currentLeadKey = 0;
    const char* _path;
    std::vector<std::string> _lines;
    int _pos = 0;
    static const int BUFFER_LEN = 256;
    char _buffer[BUFFER_LEN] = {};
    std::string _keyFormat;
};


#endif //STOREBENCHMARK_DATASOURCE_H
