//
// Created by yanhao on 2020/7/31.
//
#include "UserBehavior.h"

const int &UserBehavior::getId() const {
    return _id;
}

void UserBehavior::setId(int id){
    _id = id;
}

const int &UserBehavior::getUid() const{
    return _uid;
}

void UserBehavior::setUid(int uid){
    _uid = uid;
}

const std::string &UserBehavior::getLogTime() const{
    return _logTime;
}

void UserBehavior::setLogTime(const std::string &logTime){
    _logTime = logTime;
}

const std::string &UserBehavior::getIp() const{
    return _ip;
}

void UserBehavior::setIp(const std::string ip){
    _ip = ip;
}

const std::string &UserBehavior::getDevice() const{
    return _device;
}

void UserBehavior::setDevice(const std::string device){
    _device = device;
}


