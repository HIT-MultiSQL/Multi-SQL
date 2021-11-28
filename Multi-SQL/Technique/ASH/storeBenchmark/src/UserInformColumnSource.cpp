//
// Created by yanhao on 2020/7/31.
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
#include "UserInformColumnSource.h"
#include "UserInformSource.h"

vector<string> UserInformColumnSource::getFields() {
    vector<string> ret;
    ret.emplace_back("NAME");
    ret.emplace_back("AGE");
    ret.emplace_back("GENDER");
    ret.emplace_back("OCCUPATION");
    ret.emplace_back("REGISTER_DATE");
    return ret;
}

long UserInformColumnSource::wtGetCursorLeadKey(WT_CURSOR *cursor) const {
    uint32_t id;
    const char* name;
    uint8_t age;
    char gender;
    const char* occupation;
    const char* register_date;
    WiredTigerBench::error_check(cursor->get_value(cursor, &id, &name, &age, &gender, &occupation, &register_date), "UserInformColumnSource::wtGetCursorLeadKey");
    return id;
}

void UserInformColumnSource::wtInitLeadKeyCursor(WT_CURSOR *cursor, long leadKey) const {
#ifdef DEBUG_WY
    cout << "init:" << leadKey << endl;
#endif
    cursor->set_key(cursor, leadKey);
}

void UserInformColumnSource::initValuePool(int rowSize, int avgRange, int avgConsecutive) {
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

void UserInformColumnSource::wtSetNextValue(WT_CURSOR *cursor) {
    UserInformStruct* item = UserInformSource::parseUserInformStruct(_lines[_currentRows - 1]);
    cursor->set_value(cursor, item->id, item->name, item->age, item->gender, item->occupation, item->register_date);

    delete item;
}
