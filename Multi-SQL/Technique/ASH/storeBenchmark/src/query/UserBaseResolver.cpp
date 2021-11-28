//
// Created by yanhao on 2020/8/2.
//

#include "UserBaseResolver.h"
#include <boost/filesystem/path.hpp>
#include <boost/filesystem/operations.hpp>

UserBaseResolver::UserBaseResolver(string &informTablePath, string &behaviorTablePath,
        string &userInformPath, string& userBehaviorPath, string &workDir):
        _userInformPath(userInformPath), _userBehaviorPath(userBehaviorPath), _workDir(workDir){
    wiredtiger_open(informTablePath.c_str(), nullptr, nullptr, &_INFORMCOConn);
    _INFORMCOConn->open_session(_INFORMCOConn, nullptr, nullptr, &_INFORMCOSession);

    wiredtiger_open(behaviorTablePath.c_str(), nullptr, nullptr, &_BEHAVIORCOConn);
    _BEHAVIORCOConn->open_session(_BEHAVIORCOConn, nullptr, nullptr, &_BEHAVIORCOSession);

    boost::filesystem::path path(workDir);
    if (boost::filesystem::exists(path)) {
        system(("rm -rf " + workDir).c_str());
    }
    boost::filesystem::create_directory(path);
}

UserBaseResolver::~UserBaseResolver() {
    if (_INFORMCOSession != nullptr) {
        _INFORMCOSession->close(_INFORMCOSession, nullptr);
    }
    _INFORMCOSession = nullptr;
    if (_INFORMCOConn != nullptr) {
        _INFORMCOConn->close(_INFORMCOConn, nullptr);
    }
    _INFORMCOConn = nullptr;

    if (_BEHAVIORCOSession != nullptr) {
        _BEHAVIORCOSession->close(_BEHAVIORCOSession, nullptr);
    }
    _BEHAVIORCOSession = nullptr;
    if (_BEHAVIORCOConn != nullptr) {
        _BEHAVIORCOConn->close(_BEHAVIORCOConn, nullptr);
    }
    _BEHAVIORCOConn = nullptr;
}