//
// Created by yanhao on 2020/8/2.
//
#include "UserRowStoreResolver.h"
#include "../WiredTigerBench.h"
#include "../UserInform.h"
#include "../UserInformSource.h"
#include "../UserBehavior.h"
#include "../UserBehaviorSource.h"
#include <cstring>
#include <fstream>
#include <iostream>
#include <chrono>
#include <vector>
#include <boost/algorithm/string.hpp>
UserRowStoreResolver::UserRowStoreResolver(string& informTablePath, string& behaviorTablePath,
        string& userInformPath, string& userBehaviorPath, string &workDir) : UserBaseResolver(
        informTablePath, behaviorTablePath, userInformPath, userBehaviorPath, workDir) {
    wiredtiger_open(workDir.c_str(), nullptr, "create", &_Conn);
    _Conn->open_session(_Conn, nullptr, nullptr, &_Session);

    //参数配置
    string userInformColumnNames = "ID,NAME,AGE,GENDER,OCCUPATION,REGISTER_DATE";
    string userInformconfig = "key_format=I,value_format=SI1sSS,columns=(" + userInformColumnNames + ")";

    string userBehaviorColumnNames = "ID,UID,LOG_TIME,IP,DEVICE";
    string userBehaviorconfig = "key_format=I,value_format=ISSS,columns=(" + userBehaviorColumnNames + ")";

    //创建表userInform
    WiredTigerBench::error_check(_Session->create(_Session, "table:user_inform", userInformconfig.c_str()),
                                 "UserRowStoreResolver::loadData create user_inform");
    WiredTigerBench::error_check(_Session->create(_Session, "table:user_behavior", userBehaviorconfig.c_str()),
                                 "UserRowStoreResolver::loadData create user_behavior");
    cout << "execute row store test..." << endl;
}

void UserRowStoreResolver::loadData(unsigned long preLoad1, unsigned long actualLoad1, unsigned long preLoad2, unsigned long actualLoad2) {
    const int BUFFER_LEN = 256;
    char buffer[BUFFER_LEN] = {};
    std::fstream _fstream1;
    _fstream1.open(_userInformPath);
    vector<string> lines1;
    while (_fstream1.getline(buffer, BUFFER_LEN)) {
        if (strlen(buffer) > 0) {
            std::string line(buffer);
            lines1.push_back(line);
        }
    }
    _fstream1.close();

    if (preLoad1 > lines1.size()) {
        preLoad1 = lines1.size();
    }
    if (actualLoad1 > lines1.size() - preLoad1) {
        actualLoad1 = lines1.size() - preLoad1;
    }

    std::fstream _fstream2;
    _fstream2.open(_userBehaviorPath);
    vector<string> lines2;
    while (_fstream2.getline(buffer, BUFFER_LEN)) {
        if (strlen(buffer) > 0) {
            std::string line(buffer);
            lines2.push_back(line);
        }
    }
    _fstream2.close();

    if (preLoad2 > lines2.size()) {
        preLoad2 = lines2.size();
    }
    if (actualLoad2 > lines2.size() - preLoad2) {
        actualLoad2 = lines2.size() - preLoad2;
    }


    WT_CURSOR* _INFORMcursor = nullptr;
    WiredTigerBench::error_check(_Session->open_cursor(_Session, "table:user_inform", nullptr, "append", &_INFORMcursor),
                                 "UserRowStoreResolver::loadData 1");
    cout << "preLoading " << preLoad1 << " rows" << endl;
    for (unsigned long i = 0; i < preLoad1; i++) {
        UserInformStruct* item = UserInformSource::parseUserInformStruct(lines1[i]);

        _INFORMcursor->set_key(_INFORMcursor, item->id);
        _INFORMcursor->set_value(_INFORMcursor, item->name, item->age, item->gender, item->occupation, item->register_date);
        WiredTigerBench::error_check(_INFORMcursor->insert(_INFORMcursor), "UserRowStoreResolver::loadData 2");
        delete item;
    }
    cout << "insert for " << actualLoad1 << " rows" << endl;
    auto startTime1 = chrono::system_clock::now();
    for (unsigned long i = preLoad1; i < preLoad1 + actualLoad1; i++) {
        UserInformStruct* item = UserInformSource::parseUserInformStruct(lines1[i]);

        _INFORMcursor->set_key(_INFORMcursor, item->id);
        _INFORMcursor->set_value(_INFORMcursor, item->name, item->age, item->gender, item->occupation, item->register_date);
        WiredTigerBench::error_check(_INFORMcursor->insert(_INFORMcursor), "UserRowStoreResolver::loadData 3");
        delete item;
    }
    auto endTime1 = chrono::system_clock::now();
    auto duration1 = chrono::duration_cast<chrono::seconds>(endTime1- startTime1);
    cout << "***insert user_inform time:" << duration1.count() << "s" << endl;
    WiredTigerBench::error_check(_INFORMcursor->close(_INFORMcursor), "UserRowStoreResolver::loadData 4");
    _totalRows = preLoad1 + actualLoad1;

    WT_CURSOR* _BEHAVIORcursor = nullptr;
    WiredTigerBench::error_check(_Session->open_cursor(_Session, "table:user_behavior", nullptr, "append", &_BEHAVIORcursor),
                                 "UserRowStoreResolver::loadData 1");
    cout << "preLoading " << preLoad2 << " rows" << endl;
    for (unsigned long i = 0; i < preLoad2; i++) {
        UserBehaviorStruct* item = UserBehaviorSource::parseUserBehaviorStruct(lines2[i]);

        _BEHAVIORcursor->set_key(_BEHAVIORcursor, item->id);
        _BEHAVIORcursor->set_value(_BEHAVIORcursor, item->uid, item->logTime, item->ip, item->device);
        WiredTigerBench::error_check(_BEHAVIORcursor->insert(_BEHAVIORcursor), "UserRowStoreResolver::loadData 2");
        delete item;
    }
    cout << "insert for " << actualLoad2 << " rows" << endl;
    auto startTime2 = chrono::system_clock::now();
    for (unsigned long i = preLoad2; i < preLoad2 + actualLoad2; i++) {
        UserBehaviorStruct* item = UserBehaviorSource::parseUserBehaviorStruct(lines2[i]);

        _BEHAVIORcursor->set_key(_BEHAVIORcursor, item->id);
        _BEHAVIORcursor->set_value(_BEHAVIORcursor, item->uid, item->logTime, item->ip, item->device);
        WiredTigerBench::error_check(_BEHAVIORcursor->insert(_BEHAVIORcursor), "UserRowStoreResolver::loadData 3");
        delete item;
    }
    auto endTime2 = chrono::system_clock::now();
    auto duration2 = chrono::duration_cast<chrono::seconds>(endTime2 - startTime2);
    cout << "***insert user_behavior time:" << duration2.count() << "s" << endl;
    WiredTigerBench::error_check(_BEHAVIORcursor->close(_BEHAVIORcursor), "UserRowStoreResolver::loadData 4");
    _totalRows = preLoad2 + actualLoad2;
}

UserRowStoreResolver::~UserRowStoreResolver() {
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

void UserRowStoreResolver::createIndex() {
    WiredTigerBench::error_check(_INFORMCOSession->create(_INFORMCOSession, "index:userinform:index_id", "columns=(id),immutable"),"createIndex:0");
    WiredTigerBench::error_check(_BEHAVIORCOSession->create(_BEHAVIORCOSession, "index:userbehavior:index_uid", "columns=(uid),immutable"), "createIndex:1");
}

void UserRowStoreResolver::execQ1() {
    vector<uint32_t> uids;
    uids.emplace_back(500);
    uids.emplace_back(501);
    uids.emplace_back(502);
    uids.emplace_back(503);
    uids.emplace_back(504);
    WT_CURSOR* _index_uid_cursor = nullptr;
    WiredTigerBench::error_check(_BEHAVIORCOSession->open_cursor(_BEHAVIORCOSession, "index:userbehavior:index_uid(log_time,device)", nullptr, "overwrite=1", &_index_uid_cursor),
                                 "Q1:1");
    auto startTime = chrono::system_clock::now();
    int ret;
    vector<char*> devices;
    vector<int> device_counts;
    for(int i = 0; i < uids.size(); i++){
        _index_uid_cursor->reset(_index_uid_cursor);
        _index_uid_cursor->set_key(_index_uid_cursor, uids[i]);
        WiredTigerBench::error_check(_index_uid_cursor->search(_index_uid_cursor), "Q1:2");
        while ((ret = _index_uid_cursor->next(_index_uid_cursor)) == 0){
            char* log_time;
            char* device;
            _index_uid_cursor->get_value(_index_uid_cursor, &log_time, &device);
            if(stoi(log_time) >= 20200719 && stoi(log_time) <= 20200726 && print){
                cout << uids[i] << " " << device << endl;
            }
        }
    }
    auto endTime = chrono::system_clock::now();
    auto duration = chrono::duration_cast<chrono::milliseconds>(endTime - startTime);
    _index_uid_cursor->close(_index_uid_cursor);
    cout << "Q1 Behavior RowStore use Time:" << duration.count() << "ms" << endl;
}

void UserRowStoreResolver::execQ2() {
    vector<int> answer_ids;
    answer_ids.emplace_back(500);
    answer_ids.emplace_back(501);
    answer_ids.emplace_back(502);
    answer_ids.emplace_back(503);
    answer_ids.emplace_back(504);

    auto startTime = chrono::system_clock::now();
    auto duration1 = chrono::duration_cast<chrono::milliseconds>(startTime - startTime);
    auto duration2 = chrono::duration_cast<chrono::milliseconds>(startTime - startTime);
    WT_CURSOR * _inform_id_cursor;
    WiredTigerBench::error_check(_INFORMCOSession->open_cursor(_INFORMCOSession, "index:userinform:index_id(register_date)", nullptr, "overwrite=1", &_inform_id_cursor),
                                 "Q2:0");
    WT_CURSOR * _behavior_uid_cursor;
    WiredTigerBench::error_check(_BEHAVIORCOSession->open_cursor(_BEHAVIORCOSession, "index:userbehavior:index_uid(log_time)", nullptr, "overwrite=1", &_behavior_uid_cursor),
                                 "Q2:1");
    int ret = 0;
    for(int i = 0; i < answer_ids.size(); i++){
        auto startTime1 = chrono::system_clock::now();
        _inform_id_cursor->reset(_inform_id_cursor);
        _inform_id_cursor->set_key(_inform_id_cursor, answer_ids[i]);
        WiredTigerBench::error_check(_inform_id_cursor->search(_inform_id_cursor), "Q2:2");
        char* register_date;
        _inform_id_cursor->get_value(_inform_id_cursor, &register_date);
        auto endTime1 = chrono::system_clock::now();
        duration1 = chrono::duration_cast<chrono::milliseconds>(endTime1 - startTime1 + duration1);

        auto startTime2 = chrono::system_clock::now();
        _behavior_uid_cursor->reset(_behavior_uid_cursor);
        _behavior_uid_cursor->set_key(_behavior_uid_cursor, answer_ids[i]);
        WiredTigerBench::error_check(_behavior_uid_cursor->search(_behavior_uid_cursor), "Q2:3");
        char* log_time;
        while((ret = _behavior_uid_cursor->next(_behavior_uid_cursor)) == 0){
            _behavior_uid_cursor->get_value(_behavior_uid_cursor, &log_time);
            if(stoi(register_date) < 20120101 && stoi(log_time) == 20200725 && print){
                cout << answer_ids[i] << endl;
            }
        }
        auto endTime2 = chrono::system_clock::now();
        duration2 = chrono::duration_cast<chrono::milliseconds>(endTime2 - startTime2 + duration2);
    }
    auto endTime = chrono::system_clock::now();
    auto duration = chrono::duration_cast<chrono::milliseconds>(endTime - startTime);
    _inform_id_cursor->close(_inform_id_cursor);
    _behavior_uid_cursor->close(_behavior_uid_cursor);
    cout << "Q2 Inform RowStore use Time:" << duration1.count() << "ms" << endl;
    cout << "Q2 Behavior RowStore use Time:" << duration2.count() << "ms" << endl;
}

void UserRowStoreResolver::execQ3() {
    vector<int> answer_ids;
    answer_ids.emplace_back(500);
    answer_ids.emplace_back(501);
    answer_ids.emplace_back(502);
    answer_ids.emplace_back(503);
    answer_ids.emplace_back(504);

    auto startTime = chrono::system_clock::now();
    auto duration1 = chrono::duration_cast<chrono::milliseconds>(startTime - startTime);
    auto duration2 = chrono::duration_cast<chrono::milliseconds>(startTime - startTime);
    WT_CURSOR * _inform_id_cursor;
    WiredTigerBench::error_check(_INFORMCOSession->open_cursor(_INFORMCOSession, "index:userinform:index_id(register_date)", nullptr, "overwrite=1", &_inform_id_cursor),
                                 "Q3:0");
    WT_CURSOR * _behavior_uid_cursor;
    WiredTigerBench::error_check(_BEHAVIORCOSession->open_cursor(_BEHAVIORCOSession, "index:userbehavior:index_uid(log_time)", nullptr, "overwrite=1", &_behavior_uid_cursor),
                                 "Q3:1");
    int ret = 0;
    for(int i = 0; i < answer_ids.size(); i++){
        auto startTime1 = chrono::system_clock::now();
        _inform_id_cursor->reset(_inform_id_cursor);
        _inform_id_cursor->set_key(_inform_id_cursor, answer_ids[i]);
        WiredTigerBench::error_check(_inform_id_cursor->search(_inform_id_cursor), "Q3:2");
        char* register_date;
        _inform_id_cursor->get_value(_inform_id_cursor, &register_date);
        auto endTime1 = chrono::system_clock::now();
        duration1 = chrono::duration_cast<chrono::milliseconds>(endTime1 - startTime1 + duration1);

        auto startTime2 = chrono::system_clock::now();
        _behavior_uid_cursor->reset(_behavior_uid_cursor);
        _behavior_uid_cursor->set_key(_behavior_uid_cursor, answer_ids[i]);
        WiredTigerBench::error_check(_behavior_uid_cursor->search(_behavior_uid_cursor), "Q3:3");
        char* log_time;
        while((ret = _behavior_uid_cursor->next(_behavior_uid_cursor)) == 0){
            _behavior_uid_cursor->get_value(_behavior_uid_cursor, &log_time);
            if(stoi(register_date) < 20120101 && stoi(log_time) == 20200718 && print){
                cout << answer_ids[i] << endl;
            }
        }
        auto endTime2 = chrono::system_clock::now();
        duration2 = chrono::duration_cast<chrono::milliseconds>(endTime2 - startTime2 + duration2);
    }
    auto endTime = chrono::system_clock::now();
    auto duration = chrono::duration_cast<chrono::milliseconds>(endTime - startTime);
    _inform_id_cursor->close(_inform_id_cursor);
    _behavior_uid_cursor->close(_behavior_uid_cursor);
    cout << "Q3 Inform RowStore use Time:" << duration1.count() << "ms" << endl;
    cout << "Q3 Behavior RowStore use Time:" << duration2.count() << "ms" << endl;
}

void UserRowStoreResolver::execQ4(){
    vector<int> answer_ids;
    answer_ids.emplace_back(500);
    answer_ids.emplace_back(501);
    answer_ids.emplace_back(502);
    answer_ids.emplace_back(503);
    answer_ids.emplace_back(504);
    auto startTime = chrono::system_clock::now();
    WT_CURSOR * _inform_id_cursor;
    int ret;
    WiredTigerBench::error_check(_INFORMCOSession->open_cursor(_INFORMCOSession, "index:userinform:index_id", nullptr, "overwrite=1", &_inform_id_cursor),
                                 "Q4:2");
    for(int i = 0; i < answer_ids.size(); i++) {
        _inform_id_cursor->reset(_inform_id_cursor);
        _inform_id_cursor->set_key(_inform_id_cursor,  answer_ids[i]);
        WiredTigerBench::error_check(_inform_id_cursor->search(_inform_id_cursor), "Q4:5");
        char* name;
        int age;
        char* tmp;
        char* occupation;
        char* register_date;
        _inform_id_cursor->get_value(_inform_id_cursor, &name, &age, &tmp, &occupation, &register_date);
        char gender = tmp[0];
        if(stoi(register_date) < 20100101 && print){
            cout << answer_ids[i] << " " << name << " " << age << " " << gender << " " << occupation << " " << register_date << endl;
        }
    }
    auto endTime = chrono::system_clock::now();
    auto duration = chrono::duration_cast<chrono::milliseconds>(endTime - startTime);
    _inform_id_cursor->close(_inform_id_cursor);
    cout << "Q4 Inform RowStore use Time:" << duration.count() << "ms" << endl;
}

void UserRowStoreResolver::execQ5(){
    vector<int> answer_ids;
    answer_ids.emplace_back(500);
    answer_ids.emplace_back(501);
    answer_ids.emplace_back(502);
    answer_ids.emplace_back(503);
    answer_ids.emplace_back(504);
    auto startTime = chrono::system_clock::now();
    WT_CURSOR * _inform_id_cursor;
    int ret;
    WiredTigerBench::error_check(_INFORMCOSession->open_cursor(_INFORMCOSession, "index:userinform:index_id", nullptr, "overwrite=1", &_inform_id_cursor),
                                 "Q5:2");
    for(int i = 0; i < answer_ids.size(); i++) {
        _inform_id_cursor->reset(_inform_id_cursor);
        _inform_id_cursor->set_key(_inform_id_cursor,  answer_ids[i]);
        WiredTigerBench::error_check(_inform_id_cursor->search(_inform_id_cursor), "Q5:5");
        char* name;
        int age;
        char* tmp;
        char* occupation;
        char* register_date;
        _inform_id_cursor->get_value(_inform_id_cursor, &name, &age, &tmp, &occupation, &register_date);
        char gender = tmp[0];
        if(age >= 20 && age <= 30 && print){
            cout << answer_ids[i] << " " << name << " " << age << " " << gender << " " << occupation << " " << register_date << endl;
        }
    }
    auto endTime = chrono::system_clock::now();
    auto duration = chrono::duration_cast<chrono::milliseconds>(endTime - startTime);
    _inform_id_cursor->close(_inform_id_cursor);
    cout << "Q5 Inform RowStore use Time:" << duration.count() << "ms" << endl;
}