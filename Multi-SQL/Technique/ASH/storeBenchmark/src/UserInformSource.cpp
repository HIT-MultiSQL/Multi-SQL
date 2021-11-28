//
// Created by yanhao on 2020/7/30.
//

#include "DataSource.h"
#include "WiredTigerBench.h"
#include "UserInformSource.h"
#include <fstream>
#include <cstring>
#include <sstream>

UserInformSource::UserInformSource(const char *filePath) {
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
    sb1 << "%0" << ID1_LEN << "d";
    _keyFormat = sb1.str();
}

UserInform *UserInformSource::parseUserInform(const std::string &line) {
    auto* ret = new UserInform();
    unsigned int pos = 0;
    // get 1st
    size_t fieldStop = line.find(',', pos);
    std::string field = line.substr(pos, fieldStop - pos);
    ret->setId(stoi(field));
    pos = fieldStop + 1;
    // get 2nd
    fieldStop = line.find(',', pos);
    field = line.substr(pos, fieldStop - pos);
    ret->setName(field);
    pos = fieldStop + 1;
    // get 3rd
    fieldStop = line.find(',', pos);
    field = line.substr(pos, fieldStop - pos);
    ret->setAge(stoi(field));
    pos = fieldStop + 1;
    // get 4th
    fieldStop = line.find(',', pos);
    field = line.substr(pos, fieldStop - pos);
    ret->setGender(field[0]);
    pos = fieldStop + 1;
    // get 5th
    fieldStop = line.find(',', pos);
    field = line.substr(pos, fieldStop - pos);
    ret->setOccupation(field);
    pos = fieldStop + 1;
    // get 6th
    fieldStop = line.length();
    field = line.substr(pos, fieldStop - pos);
    ret->setRegister_date(field);
    pos = fieldStop + 1;
    return ret;
}

UserInformStruct *UserInformSource::parseUserInformStruct(const std::string &line) {
    auto* ret = new UserInformStruct();
    unsigned int pos = 0;
    // get 1st
    size_t fieldStop = line.find(',', pos);
    std::string field = line.substr(pos, fieldStop - pos);
    ret->id = stoi(field);
    pos = fieldStop + 1;
    // get 2nd
    fieldStop = line.find(',', pos);
    field = line.substr(pos, fieldStop - pos);
    ret->name = new char[fieldStop - pos + 1];
    strcpy(ret->name, field.data());
    pos = fieldStop + 1;
    // get 3rd
    fieldStop = line.find(',', pos);
    field = line.substr(pos, fieldStop - pos);
    ret->age = stoi(field);
    pos = fieldStop + 1;
    // get 4th
    fieldStop = line.find(',', pos);
    ret->gender = new char[fieldStop - pos + 1];
    strcpy(ret->gender, line.substr(pos, fieldStop - pos).data());
    pos = fieldStop + 1;
    // get 5th
    fieldStop = line.find(',', pos);
    ret->occupation = new char[fieldStop - pos + 1];
    strcpy(ret->occupation, line.substr(pos, fieldStop - pos).data());
    pos = fieldStop + 1;
    // get 6th
    fieldStop = line.length();
    ret->register_date = new char[fieldStop - pos + 1];
    strcpy(ret->register_date, line.substr(pos, fieldStop - pos).data());
    pos = fieldStop + 1;
    return ret;
}

bool UserInformSource::hasNext() const {
    return _pos < _lines.size();
}

void UserInformSource::close() {}

UserInform *UserInformSource::next() {
    std::string line = _lines[_pos];
    UserInform * ret = parseUserInform(line);
    _pos ++;
    return ret;
}

UserInformStruct *UserInformSource::nextItemStruct() {
    std::string line = _lines[_pos];
    UserInformStruct * ret = parseUserInformStruct(line);
    _pos ++;
    return ret;
}

string UserInformSource::getNextKeyString() {
    delete _currentItem;
    _currentItem = next();
    char key[USERINFORM_KEY_LEN];
    sprintf(key, _keyFormat.data(), _currentItem->getId());
    string ret(key);
    _currentLeadKey = _currentItem->getId();
    return ret;
}

long UserInformSource::wtSetNextKey(WT_CURSOR* cursor) {
    delete _currentItemStruct;
    _currentItemStruct = nextItemStruct();
    UserInformStruct* item = _currentItemStruct;
    cursor->set_key(cursor,item->id);
    _currentLeadKey = item->id;
    return item->id;
}

string UserInformSource::getNextValueString() {
    UserInform* item = _currentItem;
    stringstream ss;
    int padding = NAME_LEN - item->getName().length();
    for(int i = 0; i < padding; i++){
        ss << '#';
    }
    ss << item->getName();
    char buf[4];
    sprintf(buf, "%04d", item->getAge());
    ss << buf << item->getGender();
    padding = OCCUPATION_LEN - item->getOccupation().length();
    for(int i = 0; i < padding; i++){
        ss << '#';
    }
    ss << item->getOccupation();
    padding = REGISTER_DATE_LEN - item->getRegister_date().length();
    for(int i = 0; i < padding; i++){
        ss << '#';
    }
    ss << item->getRegister_date();
    return ss.str();
}

void UserInformSource::wtSetNextValue(WT_CURSOR* cursor) {
    UserInformStruct* item = _currentItemStruct;
    cursor->set_value(cursor,
                      item->name, item->age, item->gender, item->occupation, item->register_date);

}

long UserInformSource::wtGetCursorLeadKey(WT_CURSOR* cursor) const {
    int id;
    WiredTigerBench::error_check(cursor->get_key(cursor, &id), "UserInformSource::wtGetCursorLeadKey");
    return id;
}

void UserInformSource::wtInitLeadKeyCursor(WT_CURSOR *cursor, long leadKey) const {
    cursor->set_key(cursor, leadKey, 0, 0, 0);
}

string UserInformSource::initLeadKeyString(long leadKey) const {
    char leadKeyBuf[USERINFORM_KEY_LEN];
    sprintf(leadKeyBuf, _keyFormat.data(), leadKey, 0, 0, 0);
    return std::string(leadKeyBuf);
}

long UserInformSource::parseLeadKeyFromString(string keyString) const {
    return stoi(keyString.substr(0, ID1_LEN));
}
