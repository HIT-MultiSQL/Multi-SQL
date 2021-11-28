//
// Created by ironwei on 2020/2/22.
//

#ifndef STOREBENCHMARK_COMBINEDSOURCE_H
#define STOREBENCHMARK_COMBINEDSOURCE_H

#include "KeySource.h"
#include "ValueSource.h"
#include "DataSource.h"

class CombinedSource : public DataSource {
public:
    explicit CombinedSource(KeySource *keySource, ValueSource *valueSource, bool isRandom, int singleScanSize, int totalRows);
    string getWTKeyFormat() const final;
    string getWTValueFormat() const final;
    string getWTColumnNames() const final;
    // 获取下一个key的string形式
    string getNextKeyString() final;
    // 根据leadkey初始化key的string形式, 用于设置scan的起止leadkey
    string initLeadKeyString(long leadKey) const final;
    // 在getNextKeyString后调用，可以获取string中的leadkey。相当于perseLeadKeyFromString(getNextKeyString())
    long getCurrentLeadKey() const final;
    // 设置下一个key
    long wtSetNextKey(WT_CURSOR* cursor) final;
    // 获取下一个value的string形式
    string getNextValueString() final;
    // 设置下一个value
    void wtSetNextValue(WT_CURSOR* cursor) final;
    // 获取当前cursor对应的leadkey
    long wtGetCursorLeadKey(WT_CURSOR* cursor) const final;
    // 从key的string中获取leadkey。
    long parseLeadKeyFromString(string keyString) const final;
    // 根据leadkey初始化cursor，用于设置scan的起止leadkey
    void wtInitLeadKeyCursor(WT_CURSOR* cursor, long leadKey) const final;
    int getKeyFieldsNum() const final;
    int getValueFieldsNum() const final;
    bool hasNext() const final;
    void close() final;
    ~CombinedSource();

protected:
    long _leadKeys = 1;
    int _singleScanSize = 1;

private:
    void next();
    KeySource* _keySource;
    ValueSource* _valueSource;
    vector<int> _randomLeadKeys;
    vector<int> _randomOffsets;
    long _totalRows;
    long _currentRows;
    long _currentLeadKey = 0;
    long _currentOffset = 0;
    bool _random;
};


#endif //STOREBENCHMARK_COMBINEDSOURCE_H
