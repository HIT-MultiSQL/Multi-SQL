//
// Created by ironwei on 2020/2/22.
//

#include "KeySource.h"
#include <sstream>
#include <cmath>
#include <set>
#include "WiredTigerBench.h"

//#define DEBUG_WY

using namespace std;
void KeySource::setBitInfo(unsigned int leadKeyDecBit, unsigned int leadKeyBinBit, unsigned int offsetDecBit, unsigned int offsetBinBit) {
    _leadKeyBinBit = leadKeyBinBit;
    _leadKeyDecBit = leadKeyDecBit;
    _offsetBinBit = offsetBinBit;
    _offsetDecBit = offsetDecBit;
    setRedundancy(_redundancyBinBit);
}

void KeySource::setRedundancy(unsigned int binBit) {
    _redundancyBinBit = binBit;
    _redundancyDecBit = ceil(log10(exp2(binBit)));
}

void KeySource::setRangeInfo(int leadKeys, int singleScanSize) {
    _leadKeys = leadKeys;
    _singleScanSize = singleScanSize;
}

std::string Key1Source::generateKeyString(unsigned long leadKey, unsigned long offset) const {
    stringstream ss;
    string leadKeyStr = to_string(leadKey);
    string offsetStr = to_string(offset);
    for (int i = leadKeyStr.size(); i < _leadKeyDecBit; i++) {
        ss << "0";
    }
    ss << leadKeyStr;
    for (int i = offsetStr.size(); i < _offsetDecBit; i++) {
        ss << "0";
    }
    ss << offsetStr;
    if (offset == 0) {
        for (int i = 0; i < _redundancyDecBit; i++) {
            ss << "0";
        }
    } else {
        for (int i = 0; i < _redundancyDecBit; i++) {
            ss << (random() % 10);
        }
    }

#ifdef DEBUG_WY
    cout << "put key:" << ss.str() << endl;
#endif
    return ss.str();
}

void Key1Source::generateKeyCursor(WT_CURSOR *cursor, unsigned long leadKey, unsigned long offset) const {
    uint32_t key = (leadKey << _offsetBinBit) + offset;
    if (_redundancyBinBit > 0) {
        key = key << _redundancyBinBit;
        if (offset != 0) {
            key += (random() % (1 << _redundancyBinBit));
        }
    }
    cursor->set_key(cursor, key);
#ifdef DEBUG_WY
    cout << "put key:" << leadKey << "||" << offset << " raw:" << key << endl;
#endif
}

long Key1Source::wtGetCursorLeadKey(WT_CURSOR *cursor) const {
    uint32_t key;
    WiredTigerBench::error_check(cursor->get_key(cursor, &key), "Key1Source::wtGetCursorLeadKey");
    unsigned int leadKey = key >> _offsetBinBit;
    if (_redundancyBinBit > 0) {
        leadKey = leadKey >> _redundancyBinBit;
    }
#ifdef DEBUG_WY
    cout << "get lead key:" << leadKey << " raw:" << key << endl;
#endif
    return leadKey;
}

long Key1Source::parseLeadKeyFromString(std::string keyString) const {
    string leadKeyStr = keyString.substr(0, _leadKeyDecBit);
    long leadKey =  stol(leadKeyStr);
#ifdef DEBUG_WY
    cout << "get lead key:" << leadKey << " raw:" << keyString << endl;
#endif
    return leadKey;
}

void Key1Source::setRedundancy(unsigned int redundancyBinBit) {
    if (_leadKeyBinBit + _offsetBinBit + redundancyBinBit > 31 && _leadKeyBinBit + _offsetBinBit < 31) {
        redundancyBinBit = 31 - _leadKeyBinBit - _offsetBinBit;
    }
    KeySource::setRedundancy(redundancyBinBit);
}

std::string Key2Source::generateKeyString(unsigned long leadKey, unsigned long offset) const {
    stringstream ss;
    string leadKeyStr = to_string(leadKey);
    string offsetStr = to_string(offset);
    for (int i = leadKeyStr.size(); i < _leadKeyDecBit; i++) {
        ss << "0";
    }
    ss << leadKeyStr;
    for (int i = offsetStr.size(); i < _offsetDecBit; i++) {
        ss << "0";
    }
    ss << offsetStr;
    if (offset != 0) {
        for (int i = 0; i < _redundancyDecBit; i++) {
            ss << (random() % 10);
        }
    } else {
        for (int i = 0; i < _redundancyDecBit; i++) {
            ss << "0";
        }
    }
#ifdef DEBUG_WY
    cout << "put key:" << ss.str() << endl;
#endif
    return ss.str();
}

void Key2Source::generateKeyCursor(WT_CURSOR *cursor, unsigned long leadKey, unsigned long offset) const {
    unsigned long rawOffset = offset;
    if (_redundancyBinBit > 0) {
        rawOffset = offset << _redundancyBinBit;
        if (offset != 0) {
            rawOffset += random() % (1 << _redundancyBinBit);
        }
    }
    cursor->set_key(cursor, leadKey, rawOffset);
#ifdef DEBUG_WY
    cout << "put key:" << leadKey << "," << offset << ", rawOffset:" << rawOffset << endl;
#endif
}

long Key2Source::wtGetCursorLeadKey(WT_CURSOR *cursor) const {
    uint32_t leadKey, offset, rawOffset;
    WiredTigerBench::error_check(cursor->get_key(cursor, &leadKey, &rawOffset), "Key2Source::wtGetCursorLeadKey");
    offset = rawOffset;
    if (_redundancyBinBit > 0) {
        offset = rawOffset >> _redundancyBinBit;
    }
#ifdef DEBUG_WY
    cout << "get key:" << leadKey << "," << offset << ", rawOffset:" << rawOffset << endl;
#endif
    return leadKey;
}

long Key2Source::parseLeadKeyFromString(std::string keyString) const {
    string leadKeyStr = keyString.substr(0, _leadKeyDecBit);
    long leadKey = stol(leadKeyStr);
#ifdef DEBUG_WY
    cout << "get lead key:" << leadKey << " raw:" << keyString << endl;
#endif
    return leadKey;
}

void Key2Source::setRedundancy(unsigned int redundancyBinBit) {
    if (_offsetBinBit + redundancyBinBit > 31 && _offsetBinBit < 31) {
        redundancyBinBit = 31 - _offsetBinBit;
    }
    KeySource::setRedundancy(redundancyBinBit);
}

void Key3Source::setRangeInfo(int leadKeys, int singleScanSize) {
    KeySource::setRangeInfo(leadKeys, singleScanSize);
    _leadKeysList1.clear();
    _leadKeysList2.clear();
    _offsetList.clear();
    int vectorSize = ceil(sqrt(leadKeys + 1));
    for (int i = 0; i < vectorSize; i++) {
        _leadKeysList1.push_back(i + 1);
        _leadKeysList2.push_back(i + 1);
    }
    for (int i = 0; i < singleScanSize; i++) {
        _offsetList.push_back(i + 1);
    }
    setRedundancy(_redundancyBinBit);
    recalculateMapInfo();
}

void Key3Source::setRedundancy(unsigned int redundancyBinBit) {
    unsigned int actualRedundancyBit = redundancyBinBit;
    int keyListBit = ceil(log2(_leadKeysList1.size() + 1));
    if (redundancyBinBit + keyListBit > 30) {
        actualRedundancyBit = 30 - keyListBit;
    }
    KeySource::setRedundancy(actualRedundancyBit);
    if (keyListBit < 1) {
        return;
    }

    if (actualRedundancyBit > 1) {
        unsigned int keyListSize = _leadKeysList1.size();
        unsigned int listRange = keyListSize << actualRedundancyBit;
        // make leadKeysList1 redundant
        std::set<int> randomElement;
        for (int i = 0; i < keyListSize; i++) {
            int r = (int)(random() % listRange);
            while (randomElement.find(r) != randomElement.end()) {
                r = (int)(random() % listRange);
            }
            randomElement.insert(r);
        }
        _leadKeysList1.clear();
        for (int it : randomElement) {
            _leadKeysList1.push_back(it);
        }
        // make leadKeysList2 redundant
        randomElement.clear();
        for (int i = 0; i < keyListSize; i++) {
            int r = (int)(random() % listRange);
            while (randomElement.find(r) != randomElement.end()) {
                r = (int)(random() % listRange);
            }
            randomElement.insert(r);
        }
        _leadKeysList2.clear();
        for (int it : randomElement) {
            _leadKeysList2.push_back(it);
        }
        // make offsetList redundant
        randomElement.clear();
        actualRedundancyBit = redundancyBinBit;
        if (redundancyBinBit + _offsetBinBit > 30) {
            actualRedundancyBit = 30 - _offsetBinBit;
        }
        listRange = _offsetList.size() << actualRedundancyBit;
        for (int i = 0; i < _offsetList.size(); i++) {
            int r = (int)(random() % listRange);
            while (randomElement.find(r) != randomElement.end()) {
                r = (int)(random() % listRange);
            }
            randomElement.insert(r);
        }
        _offsetList.clear();
        for (int it : randomElement) {
            _offsetList.push_back(it);
        }

        recalculateMapInfo();
    }
}

void Key3Source::recalculateMapInfo() {
    // recalculate bit info
    bit1 = ceil(log10(_leadKeysList1[_leadKeysList1.size() - 1]));
    bit2 = ceil(log10(_leadKeysList2[_leadKeysList2.size() - 1]));
    bit3 = ceil(log10(_offsetList[_offsetList.size() - 1]));
    // recalculate map info
    _map1.clear();
    _map2.clear();
    for(int i = 0; i < _leadKeysList1.size(); i++) {
        _map1.insert(pair<int, int>(_leadKeysList1[i], i));
    }
    for(int i = 0; i < _leadKeysList2.size(); i++) {
        _map2.insert(pair<int, int>(_leadKeysList2[i], i));
    }
}

std::string Key3Source::generateKeyString(unsigned long leadKey, unsigned long offset) const {
    std::stringstream ss;
    std::string str1, str2, str3;
    if (leadKey == 0) {
        str1 = "0";
        str2 = "0";
    } else {
        str1 = to_string(_leadKeysList1[leadKey / _leadKeysList1.size()]);
        str2 = to_string(_leadKeysList2[leadKey % _leadKeysList2.size()]);
    }
    if (offset == 0) {
        str3 = "0";
    } else {
        str3 = to_string(_offsetList[offset - 1]);
    }

    for (int i = str1.size(); i < bit1; i++) {
        ss << "0";
    }
    ss << str1;
    for (int i = str2.size(); i < bit2; i++) {
        ss << "0";
    }
    ss << str2;
    for (int i = str3.size(); i < bit3; i++) {
        ss << "0";
    }
    ss << str3;
#ifdef DEBUG_WY
    cout << "put key:" << ss.str() << ", leadKey:" << leadKey << endl;
#endif
    return ss.str();
}

void Key3Source::generateKeyCursor(WT_CURSOR *cursor, unsigned long leadKey, unsigned long offset) const {
    int f1, f2, f3;
    if (leadKey == 0) {
        f1 = 0;
        f2 = 0;
    } else {
        f1 = _leadKeysList1[leadKey / _leadKeysList1.size()];
        f2 = _leadKeysList2[leadKey % _leadKeysList2.size()];
    }
    if (offset == 0) {
        f3 = 0;
    } else {
        f3 = _offsetList[offset - 1];
    }
    cursor->set_key(cursor, f1, f2, f3);
#ifdef DEBUG_WY
    cout << "put key:" << f1 << "," << f2 << "," << f3 << ", leadKey:" << leadKey << endl;
#endif
}

long Key3Source::wtGetCursorLeadKey(WT_CURSOR *cursor) const {
    uint32_t f1, f2, f3;
    WiredTigerBench::error_check(cursor->get_key(cursor, &f1, &f2, &f3),  "Key3Source::wtGetCursorLeadKey");
    long leadKey = 0;
    auto iter1 = _map1.find(f1);
    if (iter1 != _map1.end()) {
        leadKey += (long)((iter1->second) * _leadKeysList1.size());
    }
    auto iter2 = _map2.find(f2);
    if (iter2 != _map2.end()) {
        leadKey += (iter2->second);
    }
#ifdef DEBUG_WY
    cout << "get key:" << leadKey << ", raw:" << f1 << "," << f2 << "," << f3 << endl;
#endif
    return leadKey;
}

long Key3Source::parseLeadKeyFromString(std::string keyString) const {
    string str1 = keyString.substr(0, bit1);
    int f1 = stoi(str1);
    string str2 = keyString.substr(bit1, bit2);
    int f2 = stoi(str2);
    long leadKey = 0;
    auto iter1 = _map1.find(f1);
    if (iter1 != _map1.end()) {
        leadKey += (long)((iter1->second) * _leadKeysList1.size());
    }
    auto iter2 = _map2.find(f2);
    if (iter2 != _map2.end()) {
        leadKey += (iter2->second);
    }
#ifdef DEBUG_WY
    cout << "get key:" << leadKey << ", raw:" << keyString << endl;
#endif
    return leadKey;
}

void Key4Source::setRangeInfo(int leadKeys, int singleScanSize) {
    KeySource::setRangeInfo(leadKeys, singleScanSize);
    _leadKeysList1.clear();
    _leadKeysList2.clear();
    _offsetList.clear();
    int vectorSize = ceil(sqrt(leadKeys + 1));
    for (int i = 0; i < vectorSize; i++) {
        _leadKeysList1.push_back(i + 1);
        _leadKeysList2.push_back(i + 1);
    }
    for (int i = 0; i < singleScanSize; i++) {
        _offsetList.push_back(i + 1);
    }
    setRedundancy(_redundancyBinBit);
    recalculateMapInfo();
}

void Key4Source::setRedundancy(unsigned int redundancyBinBit) {
    unsigned int actualRedundancyBit = redundancyBinBit;
    int keyListBit = ceil(log2(_leadKeysList1.size() + 1));
    if (redundancyBinBit + keyListBit > 30) {
        actualRedundancyBit = 30 - keyListBit;
    }
    KeySource::setRedundancy(actualRedundancyBit);
    if (keyListBit < 1) {
        return;
    }

    if (actualRedundancyBit > 1) {
        unsigned int keyListSize = _leadKeysList1.size();
        unsigned int listRange = keyListSize << actualRedundancyBit;
        // make leadKeysList1 redundant
        std::set<int> randomElement;
        for (int i = 0; i < keyListSize; i++) {
            int r = (int)(random() % listRange);
            while (randomElement.find(r) != randomElement.end()) {
                r = (int)(random() % listRange);
            }
            randomElement.insert(r);
        }
        _leadKeysList1.clear();
        for (int it : randomElement) {
            _leadKeysList1.push_back(it);
        }
        // make leadKeysList2 redundant
        randomElement.clear();
        for (int i = 0; i < keyListSize; i++) {
            int r = (int)(random() % listRange);
            while (randomElement.find(r) != randomElement.end()) {
                r = (int)(random() % listRange);
            }
            randomElement.insert(r);
        }
        _leadKeysList2.clear();
        for (int it : randomElement) {
            _leadKeysList2.push_back(it);
        }
        // make offsetList redundant
        randomElement.clear();
        actualRedundancyBit = redundancyBinBit;
        if (redundancyBinBit + _offsetBinBit > 30) {
            actualRedundancyBit = 30 - _offsetBinBit;
        }
        listRange = _offsetList.size() << actualRedundancyBit;
        for (int i = 0; i < _offsetList.size(); i++) {
            int r = (int)(random() % listRange);
            while (randomElement.find(r) != randomElement.end()) {
                r = (int)(random() % listRange);
            }
            randomElement.insert(r);
        }
        _offsetList.clear();
        for (int it : randomElement) {
            _offsetList.push_back(it);
        }

        recalculateMapInfo();
    }
}

void Key4Source::recalculateMapInfo() {
    // recalculate bit info
    bit1 = ceil(log10(_leadKeysList1[_leadKeysList1.size() - 1]));
    bit2 = ceil(log10(_leadKeysList2[_leadKeysList2.size() - 1]));
    bit3 = ceil(log10(_offsetList[_offsetList.size() - 1]));
    // recalculate map info
    _map1.clear();
    _map2.clear();
    for(int i = 0; i < _leadKeysList1.size(); i++) {
        _map1.insert(pair<int, int>(_leadKeysList1[i], i));
    }
    for(int i = 0; i < _leadKeysList2.size(); i++) {
        _map2.insert(pair<int, int>(_leadKeysList2[i], i));
    }
}

std::string Key4Source::generateKeyString(unsigned long leadKey, unsigned long offset) const {
    std::stringstream ss;
    std::string str1, str2, str3;
    if (leadKey == 0) {
        str1 = "0";
        str2 = "0";
    } else {
        str1 = to_string(_leadKeysList1[leadKey / _leadKeysList1.size()]);
        str2 = to_string(_leadKeysList2[leadKey % _leadKeysList2.size()]);
    }
    if (offset == 0) {
        str3 = "0";
    } else {
        str3 = to_string(_offsetList[offset - 1]);
    }

    for (int i = str1.size(); i < bit1; i++) {
        ss << "0";
    }
    ss << str1;
    for (int i = str2.size(); i < bit2; i++) {
        ss << "0";
    }
    ss << str2;
    for (int i = str3.size(); i < bit3; i++) {
        ss << "0";
    }
    ss << str3;
    ss << _stringList[random() % 10000];
#ifdef DEBUG_WY
    cout << "put key:" << ss.str() << ", leadKey:" << leadKey << endl;
#endif
    return ss.str();
}

void Key4Source::generateKeyCursor(WT_CURSOR *cursor, unsigned long leadKey, unsigned long offset) const {
    int f1, f2, f3;
    if (leadKey == 0) {
        f1 = 0;
        f2 = 0;
    } else {
        f1 = _leadKeysList1[leadKey / _leadKeysList1.size()];
        f2 = _leadKeysList2[leadKey % _leadKeysList2.size()];
    }
    if (offset == 0) {
        f3 = 0;
    } else {
        f3 = _offsetList[offset - 1];
    }
    string append = _stringList[random() % 10000];
    cursor->set_key(cursor, f1, f2, f3, append.c_str());
#ifdef DEBUG_WY
    cout << "put key:" << f1 << "," << f2 << "," << f3 <<  "," <<  append <<", leadKey:" << leadKey << endl;
#endif
}

long Key4Source::wtGetCursorLeadKey(WT_CURSOR *cursor) const {
    uint32_t f1, f2, f3;
    char* str;
    WiredTigerBench::error_check(cursor->get_key(cursor, &f1, &f2, &f3, &str), "Key4Source::wtGetCursorLeadKey");
    long leadKey = 0;
    auto iter1 = _map1.find(f1);
    if (iter1 != _map1.end()) {
        leadKey += (long)((iter1->second) * _leadKeysList1.size());
    }
    auto iter2 = _map2.find(f2);
    if (iter2 != _map2.end()) {
        leadKey += (iter2->second);
    }
#ifdef DEBUG_WY
    cout << "get key:" << leadKey << ", raw:" << f1 << "," << f2 << "," << f3 << "," << str << endl;
#endif
    return leadKey;
}

long Key4Source::parseLeadKeyFromString(std::string keyString) const {
    string str1 = keyString.substr(0, bit1);
    int f1 = stoi(str1);
    string str2 = keyString.substr(bit1, bit2);
    int f2 = stoi(str2);
    long leadKey = 0;
    auto iter1 = _map1.find(f1);
    if (iter1 != _map1.end()) {
        leadKey += (long)((iter1->second) * _leadKeysList1.size());
    }
    auto iter2 = _map2.find(f2);
    if (iter2 != _map2.end()) {
        leadKey += (iter2->second);
    }
#ifdef DEBUG_WY
    cout << "get key:" << leadKey << ", raw:" << keyString << endl;
#endif
    return leadKey;
}

void Key5Source::setRangeInfo(int leadKeys, int singleScanSize) {
    KeySource::setRangeInfo(leadKeys, singleScanSize);
    _leadKeysList1.clear();
    _leadKeysList2.clear();
    _offsetList.clear();
    int vectorSize = ceil(sqrt(leadKeys + 1));
    for (int i = 0; i < vectorSize; i++) {
        _leadKeysList1.push_back(i + 1);
        _leadKeysList2.push_back(i + 1);
    }
    for (int i = 0; i < singleScanSize; i++) {
        _offsetList.push_back(i + 1);
    }
    setRedundancy(_redundancyBinBit);
    recalculateMapInfo();
}

void Key5Source::setRedundancy(unsigned int redundancyBinBit) {
    unsigned int actualRedundancyBit = redundancyBinBit;
    int keyListBit = ceil(log2(_leadKeysList1.size() + 1));
    if (redundancyBinBit + keyListBit > 30) {
        actualRedundancyBit = 30 - keyListBit;
    }
    KeySource::setRedundancy(actualRedundancyBit);
    if (keyListBit < 1) {
        return;
    }

    if (actualRedundancyBit > 1) {
        unsigned int keyListSize = _leadKeysList1.size();
        unsigned int listRange = keyListSize << actualRedundancyBit;
        // make leadKeysList1 redundant
        std::set<int> randomElement;
        for (int i = 0; i < keyListSize; i++) {
            int r = (int)(random() % listRange);
            while (randomElement.find(r) != randomElement.end()) {
                r = (int)(random() % listRange);
            }
            randomElement.insert(r);
        }
        _leadKeysList1.clear();
        for (int it : randomElement) {
            _leadKeysList1.push_back(it);
        }
        // make leadKeysList2 redundant
        randomElement.clear();
        for (int i = 0; i < keyListSize; i++) {
            int r = (int)(random() % listRange);
            while (randomElement.find(r) != randomElement.end()) {
                r = (int)(random() % listRange);
            }
            randomElement.insert(r);
        }
        _leadKeysList2.clear();
        for (int it : randomElement) {
            _leadKeysList2.push_back(it);
        }
        // make offsetList redundant
        randomElement.clear();
        actualRedundancyBit = redundancyBinBit;
        if (redundancyBinBit + _offsetBinBit > 30) {
            actualRedundancyBit = 30 - _offsetBinBit;
        }
        listRange = _offsetList.size() << actualRedundancyBit;
        for (int i = 0; i < _offsetList.size(); i++) {
            int r = (int)(random() % listRange);
            while (randomElement.find(r) != randomElement.end()) {
                r = (int)(random() % listRange);
            }
            randomElement.insert(r);
        }
        _offsetList.clear();
        for (int it : randomElement) {
            _offsetList.push_back(it);
        }

        recalculateMapInfo();
    }
}

void Key5Source::recalculateMapInfo() {
    // recalculate bit info
    bit1 = ceil(log10(_leadKeysList1[_leadKeysList1.size() - 1]));
    bit2 = ceil(log10(_leadKeysList2[_leadKeysList2.size() - 1]));
    bit3 = ceil(log10(_offsetList[_offsetList.size() - 1]));
    // recalculate map info
    _map1.clear();
    _map2.clear();
    for(int i = 0; i < _leadKeysList1.size(); i++) {
        _map1.insert(pair<int, int>(_leadKeysList1[i], i));
    }
    for(int i = 0; i < _leadKeysList2.size(); i++) {
        _map2.insert(pair<int, int>(_leadKeysList2[i], i));
    }
}

std::string Key5Source::generateKeyString(unsigned long leadKey, unsigned long offset) const {
    std::stringstream ss;
    std::string str1, str2, str3, str4;
    if (leadKey == 0) {
        str1 = "0";
        str2 = "0";
    } else {
        str1 = to_string(_leadKeysList1[leadKey / _leadKeysList1.size()]);
        str2 = to_string(_leadKeysList2[leadKey % _leadKeysList2.size()]);
    }
    if (offset == 0) {
        str3 = "0";
        str4 = "0";
    } else {
        str3 = to_string(_offsetList[offset - 1]);
        str4 = to_string(random() % (int) exp10(_redundancyDecBit));
    }

    for (int i = str1.size(); i < bit1; i++) {
        ss << "0";
    }
    ss << str1;
    for (int i = str2.size(); i < bit2; i++) {
        ss << "0";
    }
    ss << str2;
    for (int i = str3.size(); i < bit3; i++) {
        ss << "0";
    }
    ss << str3;
    for (int i = str4.size(); i < _redundancyDecBit; i++) {
        ss << "0";
    }
    ss << str4;

    ss << _stringList[random() % 10000];
#ifdef DEBUG_WY
    cout << "put key:" << ss.str() << ", leadKey:" << leadKey << endl;
#endif
    return ss.str();
}

void Key5Source::generateKeyCursor(WT_CURSOR *cursor, unsigned long leadKey, unsigned long offset) const {
    int f1, f2, f3, f4;
    if (leadKey == 0) {
        f1 = 0;
        f2 = 0;
    } else {
        f1 = _leadKeysList1[leadKey / _leadKeysList1.size()];
        f2 = _leadKeysList2[leadKey % _leadKeysList2.size()];
    }
    if (offset == 0) {
        f3 = 0;
        f4 = 0;
    } else {
        f3 = _offsetList[offset - 1];
        f4 = (int)(random() % (1 << _redundancyBinBit));
    }
    string append = _stringList[random() % 10000];
    cursor->set_key(cursor, f1, f2, f3, f4, append.c_str());
#ifdef DEBUG_WY
    cout << "put key:" << f1 << "," << f2 << "," << f3 <<  "," << f4 << "," <<  append <<", leadKey:" << leadKey << endl;
#endif
}

long Key5Source::wtGetCursorLeadKey(WT_CURSOR *cursor) const {
    uint32_t f1, f2, f3, f4;
    char* str;
    WiredTigerBench::error_check(cursor->get_key(cursor, &f1, &f2, &f3, &f4, &str), "Key5Source::wtGetCursorLeadKey");
    long leadKey = 0;
    auto iter1 = _map1.find(f1);
    if (iter1 != _map1.end()) {
        leadKey += (long)((iter1->second) * _leadKeysList1.size());
    }
    auto iter2 = _map2.find(f2);
    if (iter2 != _map2.end()) {
        leadKey += (iter2->second);
    }
#ifdef DEBUG_WY
    cout << "get key:" << leadKey << ", raw:" << f1 << "," << f2 << "," << f3 << "," << f4 << "," << str << endl;
#endif
    return leadKey;
}

long Key5Source::parseLeadKeyFromString(std::string keyString) const {
    string str1 = keyString.substr(0, bit1);
    int f1 = stoi(str1);
    string str2 = keyString.substr(bit1, bit2);
    int f2 = stoi(str2);
    long leadKey = 0;
    auto iter1 = _map1.find(f1);
    if (iter1 != _map1.end()) {
        leadKey += (long)((iter1->second) * _leadKeysList1.size());
    }
    auto iter2 = _map2.find(f2);
    if (iter2 != _map2.end()) {
        leadKey += (iter2->second);
    }
#ifdef DEBUG_WY
    cout << "get key:" << leadKey << ", raw:" << keyString << endl;
#endif
    return leadKey;
}
