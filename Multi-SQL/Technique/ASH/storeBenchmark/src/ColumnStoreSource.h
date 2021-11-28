//
// Created by ironwei on 2020/3/18.
//

#ifndef STOREBENCHMARK_COLUMNSTORESOURCE_H
#define STOREBENCHMARK_COLUMNSTORESOURCE_H

#include <set>
#include "DataSource.h"
#include "ColumnStoreFormat.h"

class ColumnStoreSource : public DataSource {
public:
    explicit ColumnStoreSource(
            bool isRandom,
            int singleScanSize,
            int totalRows);
    virtual void setFormat(ColumnStoreFormat* format,
            int valueLength,
            int consecutive,
            int valueRange);
    virtual void setTargetFormat(long (*pWTGetLeadKey)(WT_CURSOR*));
    string getWTKeyFormat() const final {return "r"; }
    string getWTValueFormat() const override { return _valueFormat;}
    string getWTColumnNames() const override;
    int getIntFields() const { return _intField;}
    int getStrFields() const { return _stringField;}
    virtual vector<string> getFields();
    virtual string getLeadKeyField(){return "LEADKEY"; };
    bool isRandom() const { return _random; }
    // 设置下一个key, 注意返回leadKey
    long wtSetNextKey(WT_CURSOR* cursor) override;
    // 设置下一个value
    void wtSetNextValue(WT_CURSOR* cursor) override;
    int getKeyFieldsNum() const override {return 1;}
    int getValueFieldsNum() const override {return _valueFormat.size();}
    bool hasNext() const override;
    long wtGetCursorLeadKey(WT_CURSOR* cursor) const override;
    void wtInitLeadKeyCursor(WT_CURSOR* cursor, long leadKey) const override;

    //以下为无用函数
    string getNextKeyString() override {return string();};
    string initLeadKeyString(long leadKey) const override { return string(); }
    long getCurrentLeadKey() const override {return 0;};
    string getNextValueString() override { return string(); };
    long parseLeadKeyFromString(string keyString) const override { return 0;}

    //获取域信息
    int getValueLength(int fieldNo) { return _valueLength[fieldNo]; }
    int getConsecutive(int fieldNo) { return _consecutive[fieldNo]; }
    int getValueRange(int fieldNo) { return _valueRange[fieldNo]; }

    // 关闭和析构函数
    void close() override {};
    ~ColumnStoreSource() override = default;

protected:
    virtual void initValuePool(int rowSize, int avgRange, int avgConsecutive);
    long _leadKeys = 1;
    int _singleScanSize = 1;
    int _intField = 0;
    int _stringField = 0;
    long _totalRows;
    long _currentRows;
    void (*_pWTSetValue)(WT_CURSOR*, std::vector<int>&, std::vector<string>&) = nullptr;
    long (*_pWTGetLeadKey)(WT_CURSOR*) = nullptr;
    string _valueFormat;

private:
    void next();
    long _currentLeadKey = 0;
    long _currentOffset = 0;
    unsigned int _composedLeadKey;
    unsigned int _offsetBinBit;
    bool _random;
    std::vector<std::vector<std::string> > _strValues;
    std::vector<std::vector<int> > _intValues;
    std::vector<int> _valueLength;
    std::vector<int> _valueRange;
    std::vector<int> _consecutive;
};

class LineItemColumnSource: public ColumnStoreSource {
public:
    explicit LineItemColumnSource(bool isRandom, string& filePath): ColumnStoreSource(isRandom, 1, 11997996),
    _filePath(filePath){

    };
protected:
    void setTargetFormat(long (*pWTGetLeadKey)(WT_CURSOR*)) override {};
    // full names(include id and leadkey)
    string getWTColumnNames() const override {return "ID,ORDERKEY,PARTKEY,SUPPKEY,LINENUMBER,QUANTITY,"
                                                     "EXTENDPRICE,DISCOUNT,TAX,RETURNFLAG,LINESTATUS,SHIPDATE,"
                                                     "COMMITDATE,RECEIPTDATE,SHIPINSTRUCT,SHIPMODE,COMMENT";};
    // value names
    vector<string> getFields() override;
    string getLeadKeyField() override{return "ORDERKEY"; };
    // init vector
    void initValuePool(int rowSize, int avgRange, int avgConsecutive) override;
    void wtSetNextValue(WT_CURSOR* cursor) override;
    long wtGetCursorLeadKey(WT_CURSOR* cursor) const override;
    void wtInitLeadKeyCursor(WT_CURSOR* cursor, long leadKey) const override;

private:
    string _filePath;
    std::vector<std::string> _lines;
};

#endif //STOREBENCHMARK_COLUMNSTORESOURCE_H
