//
// Created by yanhao on 2020/7/31.
//

#include "DataSource.h"
#include "WiredTigerBench.h"
#include "UserBehavior.h"
#include "UserBehaviorSource.h"
#include <fstream>
#include <cstring>
#include <sstream>

UserBehaviorSource::UserBehaviorSource(const char *filePath) {
    _path = filePath;
    std::fstream _fstream;
    _fstream.open(_path);
    while (_fstream.getline(_buffer, BUFFER_LEN)) {
        if (strlen(_buffer) > 0) {
            std::string line(_buffer);
            _lines.push_back(line);
        }
    }
    _fstream.close();

    stringstream sb1;
    sb1 << "%0" << ID2_LEN << "d";
    _keyFormat = sb1.str();
}

UserBehavior *UserBehaviorSource::parseUserBehavior(const std::string &line) {
    auto* ret = new UserBehavior();
    unsigned int pos = 0;
    // get 1st
    size_t fieldStop = line.find(',', pos);
    std::string field = line.substr(pos, fieldStop - pos);
    ret->setId(stoi(field));
    pos = fieldStop + 1;
    // get 2nd
    fieldStop = line.find(',', pos);
    field = line.substr(pos, fieldStop - pos);
    ret->setUid(stoi(field));
    pos = fieldStop + 1;
    // get 3rd
    fieldStop = line.find(',', pos);
    field = line.substr(pos, fieldStop - pos);
    ret->setLogTime(field);
    pos = fieldStop + 1;
    // get 4th
    fieldStop = line.find(',', pos);
    field = line.substr(pos, fieldStop - pos);
    ret->setIp(field);
    pos = fieldStop + 1;
    // get 5th
    fieldStop = line.length();
    field = line.substr(pos, fieldStop - pos);
    ret->setDevice(field);
    pos = fieldStop + 1;
    return ret;
}

UserBehaviorStruct *UserBehaviorSource::parseUserBehaviorStruct(const std::string &line) {
    auto* ret = new UserBehaviorStruct();
    unsigned int pos = 0;
    // get 1st
    size_t fieldStop = line.find(',', pos);
    std::string field = line.substr(pos, fieldStop - pos);
    ret->id = stoi(field);
    pos = fieldStop + 1;
    // get 2nd
    fieldStop = line.find(',', pos);
    field = line.substr(pos, fieldStop - pos);
    ret->uid = stoi(field);
    pos = fieldStop + 1;
    // get 3rd
    fieldStop = line.find(',', pos);
    field = line.substr(pos, fieldStop - pos);
    ret->logTime = new char[field.length() + 1];
    strcpy(ret->logTime, field.c_str());
    pos = fieldStop + 1;
    // get 4th
    fieldStop = line.find(',', pos);
    ret->ip = new char[fieldStop - pos + 1];
    strcpy(ret->ip, line.substr(pos, fieldStop - pos).data());
    pos = fieldStop + 1;
    // get 5th
    fieldStop = line.length();
    ret->device = new char[fieldStop - pos + 1];
    strcpy(ret->device, line.substr(pos, fieldStop - pos).data());
    pos = fieldStop + 1;
    return ret;
}

bool UserBehaviorSource::hasNext() const {
    return _pos < _lines.size();
}

void UserBehaviorSource::close() {}

UserBehavior *UserBehaviorSource::next() {
    std::string line = _lines[_pos];
    UserBehavior * ret = parseUserBehavior(line);
    _pos ++;
    return ret;
}

UserBehaviorStruct *UserBehaviorSource::nextItemStruct() {
    std::string line = _lines[_pos];
    UserBehaviorStruct * ret = parseUserBehaviorStruct(line);
    _pos ++;
    return ret;
}

string UserBehaviorSource::getNextKeyString() {
    delete _currentItem;
    _currentItem = next();
    char key[USERBEHAVIOR_KEY_LEN];
    sprintf(key, _keyFormat.data(), _currentItem->getId());
    string ret(key);
    _currentLeadKey = _currentItem->getId();
    return ret;
}

long UserBehaviorSource::wtSetNextKey(WT_CURSOR* cursor) {
    delete _currentItemStruct;
    _currentItemStruct = nextItemStruct();
    UserBehaviorStruct* item = _currentItemStruct;
    cursor->set_key(cursor,item->id);
    _currentLeadKey = item->id;
    return item->id;
}

string UserBehaviorSource::getNextValueString() {
    UserBehavior* item = _currentItem;
    stringstream ss;
    char buf[5];
    sprintf(buf, "%05d", item->getUid());
    ss << buf;
    int padding = LOG_TIME_LEN - item->getLogTime().length();
    for(int i = 0; i < padding; i++){
        ss << '#';
    }
    ss << item->getLogTime();
    padding = IP_LEN - item->getIp().length();
    for(int i = 0; i < padding; i++){
        ss << '#';
    }
    ss << item->getIp();
    padding = DEVICE_LEN - item->getDevice().length();
    for(int i = 0; i < padding; i++){
        ss << '#';
    }
    ss << item->getDevice();
    return ss.str();
}

void UserBehaviorSource::wtSetNextValue(WT_CURSOR* cursor) {
    UserBehaviorStruct* item = _currentItemStruct;
    cursor->set_value(cursor,
                      item->uid, item->logTime, item->ip, item->device);
}

long UserBehaviorSource::wtGetCursorLeadKey(WT_CURSOR* cursor) const {
    int id;
    WiredTigerBench::error_check(cursor->get_key(cursor, &id), "UserBehaviorSource::wtGetCursorLeadKey");
    return id;
}

void UserBehaviorSource::wtInitLeadKeyCursor(WT_CURSOR *cursor, long leadKey) const {
    cursor->set_key(cursor, leadKey, 0, 0, 0);
}

string UserBehaviorSource::initLeadKeyString(long leadKey) const {
    char leadKeyBuf[USERBEHAVIOR_KEY_LEN];
    sprintf(leadKeyBuf, _keyFormat.data(), leadKey, 0, 0, 0);
    return std::string(leadKeyBuf);
}

long UserBehaviorSource::parseLeadKeyFromString(string keyString) const {
    return stoi(keyString.substr(0, ID2_LEN));
}