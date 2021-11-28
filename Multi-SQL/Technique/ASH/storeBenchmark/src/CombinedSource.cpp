//
// Created by ironwei on 2020/2/22.
//

#include "CombinedSource.h"
#include <sstream>
#include <iostream>
#include <cmath>

CombinedSource::CombinedSource(KeySource *keySource, ValueSource *valueSource, bool isRandom, int singleScanSize, int totalRows):
    _keySource(keySource), _valueSource(valueSource), _random(isRandom), _totalRows(totalRows) {
    _singleScanSize = singleScanSize;
    _leadKeys = totalRows / singleScanSize;
    _currentRows = 0;
    if (_leadKeys * singleScanSize < totalRows) {
        _leadKeys++;
    }

    if (isRandom) {
        for (int i = 0; i < singleScanSize; i++) {
            _randomOffsets.push_back(i + 1);
        }
        for (int i = 0; i < singleScanSize; i++) {
            int r = i + random() % (singleScanSize - i);
            int tmp = _randomOffsets[i];
            _randomOffsets[i] = _randomOffsets[r];
            _randomOffsets[r] = tmp;
        }
        for (int i = 0; i < _leadKeys; i++) {
            _randomLeadKeys.push_back(i + 1);
        }
        for (int i = 0; i < _leadKeys; i++) {
            int r = i + random() % (_leadKeys - i);
            int tmp = _randomLeadKeys[i];
            _randomLeadKeys[i] = _randomLeadKeys[r];
            _randomLeadKeys[r] = tmp;
        }
    }

    unsigned int leadKeyDecBit, leadKeyBinBit, offsetDecBit, offsetBinBit;
    leadKeyDecBit = ceil(log10(_leadKeys + 1));
    leadKeyBinBit = ceil(log2(_leadKeys + 1));
    offsetDecBit = ceil(log10(singleScanSize + 1));
    offsetBinBit = ceil(log2(singleScanSize + 1));
    if (leadKeyBinBit + offsetBinBit >= 30) {
        std::cout << "CombinedSource::CombinedSource(): Too much rows." << endl;
    }
    keySource->setBitInfo(leadKeyDecBit, leadKeyBinBit, offsetDecBit, offsetBinBit);
    _keySource->setRangeInfo(_leadKeys, singleScanSize);

    _valueSource->generateValues();
}

string CombinedSource::getWTKeyFormat() const {
    return _keySource->getWTKeyFormat();
}

string CombinedSource::getWTValueFormat() const {
    return _valueSource->getWTValueFormat();
}

string CombinedSource::getWTColumnNames() const {
    stringstream ss;
    for (int i = 0; i < _keySource->getKeyFieldsNum(); i++) {
        ss << "KEY_" << i << ",";
    }
    for (int i = 0; i < _valueSource->getValueFieldsNum() - 1; i++) {
        ss << "VALUE_" << i << ",";
    }
    ss << "VALUE_" << _valueSource->getValueFieldsNum() - 1;
    return ss.str();
}

string CombinedSource::getNextKeyString() {
    next();
    return _keySource->generateKeyString(_currentLeadKey, _currentOffset);
}

string CombinedSource::initLeadKeyString(long leadKey) const {
    return _keySource->generateKeyString(leadKey, 0);
}

long CombinedSource::getCurrentLeadKey() const {
    return _currentLeadKey;
}

long CombinedSource::wtSetNextKey(WT_CURSOR *cursor) {
    next();
    _keySource->generateKeyCursor(cursor, _currentLeadKey, _currentOffset);
    return _currentLeadKey;
}

string CombinedSource::getNextValueString() {
    return _valueSource->getNextValueString(_currentRows - 1);
}

void CombinedSource::wtSetNextValue(WT_CURSOR *cursor) {
    _valueSource->wtSetNextValue(cursor, _currentRows - 1);
}

long CombinedSource::wtGetCursorLeadKey(WT_CURSOR *cursor) const {
    return  _keySource->wtGetCursorLeadKey(cursor);
}

long CombinedSource::parseLeadKeyFromString(string keyString) const {
    return _keySource->parseLeadKeyFromString(keyString);
}

void CombinedSource::wtInitLeadKeyCursor(WT_CURSOR *cursor, long leadKey) const {
    _keySource->generateKeyCursor(cursor, leadKey, 0);
}

int CombinedSource::getKeyFieldsNum() const {
    return _keySource->getKeyFieldsNum();
}

int CombinedSource::getValueFieldsNum() const {
    return _valueSource->getValueFieldsNum();
}

bool CombinedSource::hasNext() const {
    return _totalRows > _currentRows;
}

void CombinedSource::close() {}

CombinedSource::~CombinedSource() {
}

void CombinedSource::next() {
    if (_random) {
        _currentLeadKey = _randomLeadKeys[_currentRows % _leadKeys];
        _currentOffset = _randomOffsets[_currentRows / _leadKeys];
    } else {
        _currentOffset = _currentRows % _singleScanSize + 1;
        _currentLeadKey = _currentRows / _singleScanSize + 1;
    }
    _currentRows++;
}


