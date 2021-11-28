//
// Created by ironwei on 2020/2/22.
//

#include "ValueSource.h"
#include <cmath>
#include <sstream>
#include "tools/Tools.h"

#define RANDOM_POOL_SIZE 50000
//#define DEBUG_WY

using namespace std;
void ValueSource::setRandomPara(unsigned int valueRange, unsigned int consecutive) {
    _valueRange.clear();
    _consecutive.clear();
    if (valueRange >= 2 && valueRange <= 63) {
        _valueRange.push_back(valueRange);
    } else {
        _valueRange.push_back(52);
    }
    if (consecutive >= 1) {
        _consecutive.push_back(consecutive);
    } else {
        _consecutive.push_back(1);
    }
    initRandomPara();
}

ValueSource::ValueSource(unsigned int valueLength) {
    _valueLength.push_back(valueLength);
    _valueRange.push_back(52);
    _consecutive.push_back(1);
}

void ValueSource::initSplitMethod() {
    if (getValueFieldsNum() <= 1) {
        return;
    }

    int totalLength = _valueLength[0];
    _valueLength.clear();
    int field = getValueFieldsNum();

    int bucket = (int)totalLength / 4;
    if (bucket < field) {
        bucket = field;
    } else if (bucket > 256) {
        bucket = 256;
    }
    bucket -= field;

    int avg, right, left;
    for (int i = 0; i < field - 1; i++) {
        avg = (int) round(1.0 * bucket / (field - i));
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
}

std::string ValueSource::getNextValueString(int pos) {
    stringstream ss;
    for (int i = 0; i < _valueLength.size(); i++) {
        ss << _values[i][pos % RANDOM_POOL_SIZE];
    }
#ifdef DEBUG_WY
    cout << "put value: " << ss.str() << endl;
#endif
    return ss.str();
}

void ValueSource::initRandomPara() {
    int maxRange = 63;
    int minRange = 2;
    int avgRange = _valueRange[0];
    int avgConsecutive = _consecutive[0];
    int field = getValueFieldsNum();
    int totalRange = 0;
    int totalConsecutive = 0;
    _valueRange.clear();
    _consecutive.clear();
    if (maxRange - avgRange >= avgRange - minRange) {
        maxRange = avgRange + (avgRange - minRange);
    } else {
        minRange = avgRange - (maxRange - avgRange);
    }
    for (int i = 0; i < field; i++) {
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

    if (totalRange > field * avgRange) {
        int overhead = totalRange - field * avgRange;
        for (int i = 0; i < field; i++){
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
    } else if (totalRange < field * avgRange) {
        int overhead = field * avgRange - totalRange;
        for (int i = 0; i < field; i++){
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

    if (totalConsecutive > field * avgConsecutive) {
        int overhead = totalConsecutive - field * avgConsecutive;
        for (int i = 0; i < field; i++){
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
    } else if (totalConsecutive < field * avgConsecutive) {
        int overhead = field * avgConsecutive - totalConsecutive;
        for (int i = 0; i < field; i++){
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
}

void ValueSource::generateValues() {
    for (int i = 0; i < getValueFieldsNum(); i++) {
        vector<string> tmp;
        _values.push_back(tmp);
    }
    for (int i = 0; i < getValueFieldsNum(); i++) {
        for (int j = 0; j < RANDOM_POOL_SIZE; j++) {
            _values[i].push_back(randomString(_valueRange[i], _consecutive[i], _valueLength[i]));
        }
    }
}

void Value1Source::wtSetNextValue(WT_CURSOR *cursor, int pos) {
    pos = pos % RANDOM_POOL_SIZE;
    string value = _values[0][pos];
#ifdef DEBUG_WY
    cout << "put value:" << value << endl;
#endif
    cursor->set_value(cursor, value.c_str());
}

void Value2Source::wtSetNextValue(WT_CURSOR *cursor, int pos) {
    pos = pos % RANDOM_POOL_SIZE;
    string value1 = _values[0][pos];
    string value2 = _values[1][pos];
#ifdef DEBUG_WY
    cout << "put value:" << value1 << ", " << value2 << endl;
#endif
    cursor->set_value(cursor, value1.c_str(), value2.c_str());
}

void Value3Source::wtSetNextValue(WT_CURSOR *cursor, int pos) {
    pos = pos % RANDOM_POOL_SIZE;
#ifdef DEBUG_WY
    cout << "put value:";
    for (int i = 0; i < getValueFieldsNum(); i++) {
        cout << _values[i][pos] << ",";
    }
    cout << endl;
#endif
    cursor->set_value(cursor, _values[0][pos].c_str(), _values[1][pos].c_str(), _values[2][pos].c_str());
}

void Value4Source::wtSetNextValue(WT_CURSOR *cursor, int pos) {
    pos = pos % RANDOM_POOL_SIZE;
#ifdef DEBUG_WY
    cout << "put value:";
    for (int i = 0; i < getValueFieldsNum(); i++) {
        cout << _values[i][pos] << ",";
    }
    cout << endl;
#endif
    cursor->set_value(cursor, _values[0][pos].c_str(), _values[1][pos].c_str(), _values[2][pos].c_str(), _values[3][pos].c_str());
}

void Value6Source::wtSetNextValue(WT_CURSOR *cursor, int pos) {
    pos = pos % RANDOM_POOL_SIZE;
#ifdef DEBUG_WY
    cout << "put value:";
    for (int i = 0; i < getValueFieldsNum(); i++) {
        cout << _values[i][pos] << ",";
    }
    cout << endl;
#endif
    cursor->set_value(cursor,_values[0][pos].c_str(), _values[1][pos].c_str(), _values[2][pos].c_str(), _values[3][pos].c_str(),
            _values[4][pos].c_str(), _values[5][pos].c_str());
}

void Value8Source::wtSetNextValue(WT_CURSOR *cursor, int pos) {
    pos = pos % RANDOM_POOL_SIZE;
#ifdef DEBUG_WY
    cout << "put value:";
    for (int i = 0; i < getValueFieldsNum(); i++) {
        cout << _values[i][pos] << ",";
    }
    cout << endl;
#endif
    cursor->set_value(cursor,_values[0][pos].c_str(), _values[1][pos].c_str(), _values[2][pos].c_str(), _values[3][pos].c_str(),
                      _values[4][pos].c_str(), _values[5][pos].c_str(), _values[6][pos].c_str(), _values[7][pos].c_str());
}

void Value10Source::wtSetNextValue(WT_CURSOR *cursor, int pos) {
    pos = pos % RANDOM_POOL_SIZE;
#ifdef DEBUG_WY
    cout << "put value:";
    for (int i = 0; i < getValueFieldsNum(); i++) {
        cout << _values[i][pos] << ",";
    }
    cout << endl;
#endif
    cursor->set_value(cursor,_values[0][pos].c_str(), _values[1][pos].c_str(), _values[2][pos].c_str(), _values[3][pos].c_str(),
                      _values[4][pos].c_str(), _values[5][pos].c_str(), _values[6][pos].c_str(), _values[7][pos].c_str(),
                      _values[8][pos].c_str(), _values[9][pos].c_str());
}

void Value12Source::wtSetNextValue(WT_CURSOR *cursor, int pos) {
    pos = pos % RANDOM_POOL_SIZE;
#ifdef DEBUG_WY
    cout << "put value:";
    for (int i = 0; i < getValueFieldsNum(); i++) {
        cout << _values[i][pos] << ",";
    }
    cout << endl;
#endif
    cursor->set_value(cursor,_values[0][pos].c_str(), _values[1][pos].c_str(), _values[2][pos].c_str(), _values[3][pos].c_str(),
                      _values[4][pos].c_str(), _values[5][pos].c_str(), _values[6][pos].c_str(), _values[7][pos].c_str(),
                      _values[8][pos].c_str(), _values[9][pos].c_str(), _values[10][pos].c_str(), _values[11][pos].c_str());
}

void Value16Source::wtSetNextValue(WT_CURSOR *cursor, int pos) {
    pos = pos % RANDOM_POOL_SIZE;
#ifdef DEBUG_WY
    cout << "put value:";
    for (int i = 0; i < getValueFieldsNum(); i++) {
        cout << _values[i][pos] << ",";
    }
    cout << endl;
#endif
    cursor->set_value(cursor,_values[0][pos].c_str(), _values[1][pos].c_str(), _values[2][pos].c_str(), _values[3][pos].c_str(),
                      _values[4][pos].c_str(), _values[5][pos].c_str(), _values[6][pos].c_str(), _values[7][pos].c_str(),
                      _values[8][pos].c_str(), _values[9][pos].c_str(), _values[10][pos].c_str(), _values[11][pos].c_str(),
                      _values[12][pos].c_str(), _values[13][pos].c_str(), _values[14][pos].c_str(), _values[15][pos].c_str());
}

void Value20Source::wtSetNextValue(WT_CURSOR *cursor, int pos) {
    pos = pos % RANDOM_POOL_SIZE;
#ifdef DEBUG_WY
    cout << "put value:";
    for (int i = 0; i < getValueFieldsNum(); i++) {
        cout << _values[i][pos] << ",";
    }
    cout << endl;
#endif
    cursor->set_value(cursor,_values[0][pos].c_str(), _values[1][pos].c_str(), _values[2][pos].c_str(), _values[3][pos].c_str(),
                      _values[4][pos].c_str(), _values[5][pos].c_str(), _values[6][pos].c_str(), _values[7][pos].c_str(),
                      _values[8][pos].c_str(), _values[9][pos].c_str(), _values[10][pos].c_str(), _values[11][pos].c_str(),
                      _values[12][pos].c_str(), _values[13][pos].c_str(), _values[14][pos].c_str(), _values[15][pos].c_str(),
                      _values[16][pos].c_str(), _values[17][pos].c_str(), _values[18][pos].c_str(), _values[19][pos].c_str());
}
