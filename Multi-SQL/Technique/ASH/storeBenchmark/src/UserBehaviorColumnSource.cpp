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
#include "UserBehaviorColumnSource.h"
#include "UserBehaviorSource.h"

vector<string> UserBehaviorColumnSource::getFields() {
    vector<string> ret;
    ret.emplace_back("UID");
    ret.emplace_back("LOG_TIME");
    ret.emplace_back("IP");
    ret.emplace_back("DEVICE");
    return ret;
}

long UserBehaviorColumnSource::wtGetCursorLeadKey(WT_CURSOR *cursor) const {
    uint32_t id;
    uint32_t uid;
    const char* log_time;
    const char* ip;
    const char* device;
    WiredTigerBench::error_check(cursor->get_value(cursor, &id, &uid, &log_time, &ip, &device), "UserBehaviorColumnSource::wtGetCursorLeadKey");
    return id;
}

void UserBehaviorColumnSource::wtInitLeadKeyCursor(WT_CURSOR *cursor, long leadKey) const {
#ifdef DEBUG_WY
    cout << "init:" << leadKey << endl;
#endif
    cursor->set_key(cursor, leadKey);
}

void UserBehaviorColumnSource::initValuePool(int rowSize, int avgRange, int avgConsecutive) {
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

void UserBehaviorColumnSource::wtSetNextValue(WT_CURSOR *cursor) {
    UserBehaviorStruct* item = UserBehaviorSource::parseUserBehaviorStruct(_lines[_currentRows - 1]);
    cursor->set_value(cursor, item->id, item->uid, item->logTime, item->ip, item->device);

    delete item;
}

