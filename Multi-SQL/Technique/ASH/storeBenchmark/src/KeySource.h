//
// Created by ironwei on 2020/2/22.
//

#ifndef STOREBENCHMARK_KEYSOURCE_H
#define STOREBENCHMARK_KEYSOURCE_H

#include <string>
#include <vector>
#include <map>
#include "wiredtiger.h"
#include "wiredtiger_ext.h"
#include "tools/Tools.h"

class KeySource {
public:
    explicit KeySource(int keyFields) : _fields(keyFields) {};
    virtual std::string getWTKeyFormat() const = 0;
    /**
     *
     * @param leadKey >0: actuall data =0: before first row of table
     * @param offset >0: actuall data. =0: before first row of given leadkey
     * @return
     */
    virtual std::string generateKeyString(unsigned long leadKey, unsigned long offset) const = 0;
    /**
     *
     * @param leadKey >0: actuall data =0: before first row of table
     * @param offset >0: actuall data. =0: before first row of given leadkey
     * @return
     */
    virtual void generateKeyCursor(WT_CURSOR* cursor, unsigned long leadKey, unsigned long offset) const = 0;
    virtual long wtGetCursorLeadKey(WT_CURSOR* cursor) const = 0;
    virtual long parseLeadKeyFromString(std::string keyString) const = 0;
    int getKeyFieldsNum() const {return _fields;};
    virtual int getKeyLength() const = 0;
    unsigned int getActualRedundancy() { return _redundancyDecBit; };
    void setBitInfo(unsigned int leadKeyDecBit, unsigned int leadKeyBinBit, unsigned int offsetDecBit, unsigned int offsetBinBit);
    virtual void setRangeInfo(int leadKeys, int singleScanSize);
    virtual void setRedundancy(unsigned int redundancyBinBit);
    virtual ~KeySource() = default;

protected:
    int _fields;
    int _leadKeys = 0;
    int _singleScanSize = 0;
    unsigned int _leadKeyDecBit = 0;
    unsigned int _leadKeyBinBit = 0;
    unsigned int _offsetDecBit = 0;
    unsigned int _offsetBinBit = 0;
    unsigned int _redundancyDecBit = 0;
    unsigned int _redundancyBinBit = 0;
};

class Key1Source : public KeySource {
public:
    explicit Key1Source() : KeySource(1) {};
    std::string getWTKeyFormat() const final {return std::string("I"); };
    std::string generateKeyString(unsigned long leadKey, unsigned long offset) const override;
    void generateKeyCursor(WT_CURSOR* cursor, unsigned long leadKey, unsigned long offset) const override;
    long wtGetCursorLeadKey(WT_CURSOR* cursor) const override;
    long parseLeadKeyFromString(std::string keyString) const override;
    void setRedundancy(unsigned int redundancyBinBit) override;
    int getKeyLength() const override {return 4;}
    ~Key1Source() override = default;
};

class Key2Source : public KeySource {
public:
    explicit Key2Source() : KeySource(2) {};
    std::string getWTKeyFormat() const final {return std::string("II"); };
    std::string generateKeyString(unsigned long leadKey, unsigned long offset) const override;
    void generateKeyCursor(WT_CURSOR* cursor, unsigned long leadKey, unsigned long offset) const override;
    long wtGetCursorLeadKey(WT_CURSOR* cursor) const override;
    long parseLeadKeyFromString(std::string keyString) const override;
    void setRedundancy(unsigned int redundancyBinBit) override;
    int getKeyLength() const override {return 8;}
    ~Key2Source() override = default;
};

class Key3Source : public KeySource {
public:
    explicit Key3Source() : KeySource(3) {};
    std::string getWTKeyFormat() const final {return std::string("III"); };
    std::string generateKeyString(unsigned long leadKey, unsigned long offset) const override;
    void generateKeyCursor(WT_CURSOR* cursor, unsigned long leadKey, unsigned long offset) const override;
    long wtGetCursorLeadKey(WT_CURSOR* cursor) const override;
    long parseLeadKeyFromString(std::string keyString) const override;
    void setRangeInfo(int leadKeys, int singleScanSize) override ;
    void setRedundancy(unsigned int redundancyBinBit) override;
    int getKeyLength() const override {return 12;}
    ~Key3Source() override = default;

private:
    void recalculateMapInfo();
    std::vector<int> _leadKeysList1;
    std::vector<int> _leadKeysList2;
    std::vector<int> _offsetList;
    std::map<int, int> _map1;
    std::map<int, int> _map2;
    int bit1 = 0;
    int bit2 = 0;
    int bit3 = 0;
};

class Key4Source : public KeySource {
public:
    explicit Key4Source(int stringLen) : KeySource(4) {
        for (int i = 0; i < 10000; i++) {
            _stringList.push_back(randomString(16, 1, stringLen));
        }
    };
    std::string getWTKeyFormat() const final {return std::string("IIIS"); };
    std::string generateKeyString(unsigned long leadKey, unsigned long offset) const override;
    void generateKeyCursor(WT_CURSOR* cursor, unsigned long leadKey, unsigned long offset) const override;
    long wtGetCursorLeadKey(WT_CURSOR* cursor) const override;
    long parseLeadKeyFromString(std::string keyString) const override;
    void setRangeInfo(int leadKeys, int singleScanSize) override ;
    void setRedundancy(unsigned int redundancyBinBit) override;
    int getKeyLength() const override {return 12 + _stringList[0].length();}
    ~Key4Source() override = default;
private:
    void recalculateMapInfo();
    std::vector<int> _leadKeysList1;
    std::vector<int> _leadKeysList2;
    std::vector<int> _offsetList;
    std::map<int, int> _map1;
    std::map<int, int> _map2;
    int bit1 = 0;
    int bit2 = 0;
    int bit3 = 0;
    std::vector<std::string> _stringList;
};

class Key5Source : public KeySource {
public:
    explicit Key5Source(int stringLen) : KeySource(5) {
        for (int i = 0; i < 10000; i++) {
            _stringList.push_back(randomString(16, 1, stringLen));
        }
    };
    std::string getWTKeyFormat() const final {return std::string("IIIIS"); };
    std::string generateKeyString(unsigned long leadKey, unsigned long offset) const override;
    void generateKeyCursor(WT_CURSOR* cursor, unsigned long leadKey, unsigned long offset) const override;
    long wtGetCursorLeadKey(WT_CURSOR* cursor) const override;
    long parseLeadKeyFromString(std::string keyString) const override;
    void setRangeInfo(int leadKeys, int singleScanSize) override ;
    void setRedundancy(unsigned int redundancyBinBit) override;
    int getKeyLength() const override {return 16 + _stringList[0].length();}
    ~Key5Source() override = default;
private:
    void recalculateMapInfo();
    std::vector<int> _leadKeysList1;
    std::vector<int> _leadKeysList2;
    std::vector<int> _offsetList;
    std::map<int, int> _map1;
    std::map<int, int> _map2;
    int bit1 = 0;
    int bit2 = 0;
    int bit3 = 0;
    std::vector<std::string> _stringList;
};

#endif //STOREBENCHMARK_KEYSOURCE_H
