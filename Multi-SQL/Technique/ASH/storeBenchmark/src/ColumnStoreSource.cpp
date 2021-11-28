//
// Created by ironwei on 2020/3/18.
//

#include "ColumnStoreSource.h"
#include "tools/Tools.h"
#include "WiredTigerBench.h"
#include <cmath>
#include <iostream>
#include <sstream>
#include <fstream>
#include <cstring>
#include "boost/algorithm/string.hpp"

//#define DEBUG_WY
#define RANDOM_POOL_SIZE 10000

ColumnStoreSource::ColumnStoreSource(bool isRandom,
                                     int singleScanSize,
                                     int totalRows):
                                     _singleScanSize(singleScanSize),
                                     _random(isRandom),
                                     _totalRows(totalRows){
    _leadKeys = totalRows / singleScanSize;
    _currentRows = 0;
    if (_leadKeys * singleScanSize < totalRows) {
        _leadKeys++;
    }

    unsigned int leadKeyBinBit;
    leadKeyBinBit = ceil(log2(_leadKeys + 1));
    _offsetBinBit = ceil(log2(singleScanSize + 1));
    if (leadKeyBinBit + _offsetBinBit >= 30) {
        std::cout << "ColumnStoreSource::ColumnStoreSource(): Too much rows." << endl;
    }

}

bool ColumnStoreSource::hasNext() const {
    return _totalRows > _currentRows;
}

void ColumnStoreSource::next() {
    if (_random) {
        _currentLeadKey = _currentRows % _leadKeys + 1;
        _currentOffset = _currentRows / _leadKeys + 1;
    } else {
        _currentOffset = _currentRows % _singleScanSize + 1;
        _currentLeadKey = _currentRows / _singleScanSize + 1;
    }
    _currentRows++;

    _composedLeadKey = ((unsigned long)_currentLeadKey << _offsetBinBit) + _currentOffset;
}

long ColumnStoreSource::wtSetNextKey(WT_CURSOR *cursor) {
    next();
    cursor->set_key(cursor, _currentRows);
#ifdef DEBUG_WY
    cout << "put key:" << _currentLeadKey << "||" << _currentOffset << " rec:" << _currentRows << endl;
#endif
    return _currentLeadKey;
}

string ColumnStoreSource::getWTColumnNames() const {
    stringstream ss;
    ss << "ID,LEADKEY,";
    if (_intField > 0) {
        for (int i = 0; i < _intField - 1; i++) {
            ss << "INT_" << i << ",";
        }
        ss << "INT_" << _intField - 1;
        if (_stringField > 0) {
            ss << ",";
        }
    }
    if (_stringField > 0) {
        for (int i = 0; i < _stringField - 1; i++) {
            ss << "STR_" << i << ",";
        }
        ss << "STR_" << _stringField - 1;
    }

    return ss.str();
}

void ColumnStoreSource::initValuePool(int rowSize, int avgRange, int avgConsecutive) {
    _valueLength.clear();
    _valueRange.clear();
    _consecutive.clear();

    rowSize -= (_intField * 4);
    if (rowSize < _stringField * 4) {
        rowSize = _stringField * 4;
    }
    int bucket = (int)rowSize / 4;
    if (bucket > 256) {
        bucket = 256;
    }
    bucket -= _stringField;

    int avg, right, left;
    for (int i = 0; i < _stringField - 1; i++) {
        avg = (int) round(1.0 * bucket / (_stringField - i));
        if (avg - 0 <= bucket - avg) {
            left = 0;
            right = avg * 2;
        } else {
            right = bucket;
            left = avg - (bucket - avg);
        }
        if (left == right) {
            _valueLength.push_back(4);
        } else {
            int r = random() % (right - left + 1) + left;
            bucket -= r;
            _valueLength.push_back(4 + r * 4);
        }
    }
    _valueLength.push_back(bucket * 4 + 4);

#ifdef DEBUG_WY
    cout << "split: ";
    for (int i : _valueLength) {
        cout << i << ", ";
    }
    cout << endl;
#endif

    int maxRange = 63;
    int minRange = 2;
    int totalRange = 0;
    int totalConsecutive = 0;

    if (maxRange - avgRange >= avgRange - minRange) {
        maxRange = avgRange + (avgRange - minRange);
    } else {
        minRange = avgRange - (maxRange - avgRange);
    }
    for (int i = 0; i < _stringField; i++) {
        if (maxRange == minRange) {
            _valueRange.push_back(avgRange);
        } else {
            _valueRange.push_back(random() % (maxRange - minRange + 1) + minRange);
        }
        totalRange += _valueRange[i];
        if (avgConsecutive > 1) {
            _consecutive.push_back(random() % (2 * avgConsecutive - 1) + 1);
        } else {
            _consecutive.push_back(1);
        }
        totalConsecutive += _consecutive[i];
    }

    if (totalRange > _stringField * avgRange) {
        int overhead = totalRange - _stringField * avgRange;
        for (int i = 0; i < _stringField; i++){
            if (_valueRange[i] > avgRange) {
                if (_valueRange[i] - avgRange > overhead) {
                    _valueRange[i] = _valueRange[i] - overhead;
                    break;
                } else {
                    overhead -= (_valueRange[i] - avgRange);
                    _valueRange[i] = avgRange;
                }
            }
        }
    } else if (totalRange < _stringField * avgRange) {
        int overhead = _stringField * avgRange - totalRange;
        for (int i = 0; i < _stringField; i++){
            if (_valueRange[i] < avgRange) {
                if (avgRange - _valueRange[i] > overhead) {
                    _valueRange[i] = _valueRange[i] + overhead;
                    break;
                } else {
                    overhead -= (avgRange - _valueRange[i]);
                    _valueRange[i] = avgRange;
                }
            }
        }
    }

    if (totalConsecutive > _stringField * avgConsecutive) {
        int overhead = totalConsecutive - _stringField * avgConsecutive;
        for (int i = 0; i < _stringField; i++){
            if (_consecutive[i] > avgConsecutive) {
                if (_consecutive[i] - avgConsecutive > overhead) {
                    _consecutive[i] = _consecutive[i] - overhead;
                    break;
                } else {
                    overhead -= (_consecutive[i] - avgConsecutive);
                    _consecutive[i] = avgConsecutive;
                }
            }
        }
    } else if (totalConsecutive < _stringField * avgConsecutive) {
        int overhead = _stringField * avgConsecutive - totalConsecutive;
        for (int i = 0; i < _stringField; i++){
            if (_consecutive[i] < avgConsecutive) {
                if (avgConsecutive - _consecutive[i] > overhead) {
                    _consecutive[i] = _consecutive[i] + overhead;
                    break;
                } else {
                    overhead -= (avgConsecutive - _consecutive[i]);
                    _consecutive[i] = avgConsecutive;
                }
            }
        }
    }
#ifdef DEBUG_WY
    cout << "range: ";
    for (int i : _valueRange) {
        cout << i << ", ";
    }
    cout << endl << "consecutive: ";
    for (int i : _consecutive) {
        cout << i << ", ";
    }
    cout << endl;
#endif

    for (int i = 0; i < _stringField; i++) {
        vector<string> tmp;
        _strValues.push_back(tmp);
    }
    for (int i = 0; i < _stringField; i++) {
        for (int j = 0; j < RANDOM_POOL_SIZE; j++) {
            _strValues[i].push_back(randomString(_valueRange[i], _consecutive[i], _valueLength[i]));
        }
    }

    for (int i = 0; i < _intField - 1; i++) {
        vector<int> tmp;
        _intValues.push_back(tmp);
    }
    for (int i = 0; i < _intField - 1; i++) {
        int avgRLE = random() % 10 + 1;
        while (_intValues[i].size() < RANDOM_POOL_SIZE) {
            int rle = random() % (2 * avgRLE - 1) + 1;
            int value = random() % 100000;
            for (int j = 0; j < rle; j++) {
                _intValues[i].push_back(value++);
            }
        }
    }
}

void ColumnStoreSource::setFormat(ColumnStoreFormat* format,
                                  int valueLength, int consecutive, int valueRange) {
    _valueFormat = format->getFormat();
    _pWTSetValue = format->pWTSetValue;
    _intField = format->getIntFields();
    _stringField = format->getStrFields();
    initValuePool(valueLength, valueRange, consecutive);
}

void ColumnStoreSource::wtSetNextValue(WT_CURSOR *cursor) {
    vector<int> intFields;
    vector<string> stringFields;


    intFields.push_back(_composedLeadKey);
    for (auto & _intValue : _intValues) {
        intFields.push_back(_intValue[_currentRows % RANDOM_POOL_SIZE]);
    }
    for (auto & _strValue : _strValues) {
        stringFields.push_back(_strValue[_currentRows % RANDOM_POOL_SIZE]);
    }
#ifdef DEBUG_WY
    cout << "leadKey:" << (_composedLeadKey >> _offsetBinBit) << " raw:";
    for (auto v: intFields) {
        cout << v << " ";
    }
    for (auto v: stringFields) {
        cout << v << " ";
    }
    cout << endl;
#endif
    _pWTSetValue(cursor, intFields, stringFields);
}

long ColumnStoreSource::wtGetCursorLeadKey(WT_CURSOR *cursor) const {
    long composeLeadKey = _pWTGetLeadKey(cursor);
#ifdef DEBUG_WY
    cout << "leadKey:" << (composeLeadKey >> _offsetBinBit) << endl;
#endif
    return composeLeadKey >> _offsetBinBit;
}

void ColumnStoreSource::wtInitLeadKeyCursor(WT_CURSOR *cursor, long leadKey) const {
#ifdef DEBUG_WY
    cout << "init:" << leadKey << endl;
#endif
    uint32_t key = (unsigned long)leadKey << _offsetBinBit;
    cursor->set_key(cursor, key);
}

void ColumnStoreSource::setTargetFormat(long (*pWTGetLeadKey)(WT_CURSOR *)) {
    _pWTGetLeadKey = pWTGetLeadKey;
}

vector<string> ColumnStoreSource::getFields() {
    vector<string> ret;
    for (int i = 0; i < _intField; i++) {
        ret.push_back(string("INT_") + to_string(i));
    }
    for (int i = 0; i < _stringField; i++) {
        ret.push_back(string("STR_") + to_string(i));
    }
    return ret;
}

vector<string> LineItemColumnSource::getFields() {
    vector<string> ret;
    ret.emplace_back("PARTKEY");
    ret.emplace_back("SUPPKEY");
    ret.emplace_back("LINENUMBER");
    ret.emplace_back("QUANTITY");
    ret.emplace_back("EXTENDPRICE");
    ret.emplace_back("DISCOUNT");
    ret.emplace_back("TAX");
    ret.emplace_back("RETURNFLAG");
    ret.emplace_back("LINESTATUS");
    ret.emplace_back("SHIPDATE");
    ret.emplace_back("COMMITDATE");
    ret.emplace_back("RECEIPTDATE");
    ret.emplace_back("SHIPINSTRUCT");
    ret.emplace_back("SHIPMODE");
    ret.emplace_back("COMMENT");
    return ret;
}

long LineItemColumnSource::wtGetCursorLeadKey(WT_CURSOR *cursor) const {
    uint32_t orderKey;
    uint32_t partKey;
    uint32_t suppKey;
    uint16_t lineNumber;
    uint16_t quantity;
    const char* extendPrice;
    const char* discount;
    const char* tax;
    const char* returnFlag;
    const char* lineStatus;
    const char* shipDate;
    const char* commitDate;
    const char* receiptDate;
    const char* shipInstruct;
    const char* shipMode;
    const char* comment;
    WiredTigerBench::error_check(cursor->get_value(cursor, &orderKey, &partKey, &suppKey, &lineNumber,
            &quantity, &extendPrice, &discount, &tax, &returnFlag, &lineStatus, &shipDate, &commitDate,
            &receiptDate, &shipInstruct, &shipMode, &comment), "LineItemColumnSource::wtGetCursorLeadKey");
    return orderKey;
}

void LineItemColumnSource::wtInitLeadKeyCursor(WT_CURSOR *cursor, long leadKey) const {
#ifdef DEBUG_WY
    cout << "init:" << leadKey << endl;
#endif
    cursor->set_key(cursor, leadKey);
}

void LineItemColumnSource::initValuePool(int rowSize, int avgRange, int avgConsecutive) {
    const int BUFFER_LEN = 256;
    char buffer[BUFFER_LEN] = {};
    std::fstream _fstream;
    _fstream.open(_filePath);
    while (_fstream.getline(buffer, BUFFER_LEN)) {
        if (strlen(buffer) > 0) {
            _lines.emplace_back(buffer);
        }
    }
    _fstream.close();
}

void LineItemColumnSource::wtSetNextValue(WT_CURSOR *cursor) {
    LineItemStruct* item = LineItemSource::parseLineItemStruct(_lines[_currentRows - 1]);
    cursor->set_value(cursor, item->orderKey, item->partKey, item->suppKey, item->lineNumber, item->quantity,
                      item->extendPrice, item->discount, item->tax, item->returnFlag,
                      item->lineStatus, item->shipDate, item->commitDate, item->receiptDate,
                      item->shipInstruct, item->shipMode, item->comment);

    delete item;
}
