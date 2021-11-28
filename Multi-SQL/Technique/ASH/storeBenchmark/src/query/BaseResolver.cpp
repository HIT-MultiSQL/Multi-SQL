//
// Created by ironwei on 2020/5/16.
//

#include "BaseResolver.h"
#include <boost/filesystem/path.hpp>
#include <boost/filesystem/operations.hpp>

BaseResolver::BaseResolver(string &tpcTablePath, string &lineItemPath, string &workDir):
    _lineItemPath(lineItemPath), _workDir(workDir){
    wiredtiger_open(tpcTablePath.c_str(), nullptr, nullptr, &_COConn);
    _COConn->open_session(_COConn, nullptr, nullptr, &_COSession);

    boost::filesystem::path path(workDir);
    if (boost::filesystem::exists(path)) {
        system(("rm -rf " + workDir).c_str());
    }
    boost::filesystem::create_directory(path);
}

BaseResolver::~BaseResolver() {
    if (_COSession != nullptr) {
        _COSession->close(_COSession, nullptr);
    }
    _COSession = nullptr;
    if (_COConn != nullptr) {
        _COConn->close(_COConn, nullptr);
    }
    _COConn = nullptr;
}
