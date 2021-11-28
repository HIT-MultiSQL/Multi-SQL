//
// Created by yanhao on 2020/8/4.
//
#include "UserColumnStoreResolver.h"
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
UserColumnStoreResolver::UserColumnStoreResolver(string& informTablePath, string& behaviorTablePath,
        string& userInformPath, string& userBehaviorPath, string &workDir) : UserBaseResolver(
        informTablePath, behaviorTablePath, userInformPath, userBehaviorPath, workDir) {
    wiredtiger_open(workDir.c_str(), nullptr, "create", &_Conn);
    _Conn->open_session(_Conn, nullptr, nullptr, &_Session);

    //参数配置
    string userInformColumnNames = "ID,NAME,AGE,GENDER,OCCUPATION,REGISTER_DATE";
    string userInformColgroups = userInformColumnNames;
    boost::algorithm::to_lower(userInformColgroups);
    vector<string> userInformColumnList;
    vector<string> userInformColgroupList;
    boost::algorithm::split(userInformColumnList, userInformColumnNames, boost::is_any_of( "," ));
    boost::algorithm::split(userInformColgroupList, userInformColgroups, boost::is_any_of( "," ));
    string userInformconfig = "key_format=r,value_format=ISI1sSS,columns=(MYID," + userInformColumnNames + "),colgroups=(" + userInformColgroups + ")";
    WiredTigerBench::error_check(_INFORMCOSession->create(_INFORMCOSession, "table:userinformcol", userInformconfig.c_str()),
                                 "ColumnStoreResolver::loadData create userinformcol");
//    _INFORMCOSession->drop(_INFORMCOSession, "table:userinformcol", nullptr);
    for (int i = 0; i < userInformColumnList.size(); i++) {
        string title = "colgroup:userinformcol:" + userInformColgroupList[i];
        userInformconfig = "columns=(" + userInformColumnList[i] + ")";
//        _INFORMCOSession->drop(_INFORMCOSession, title.c_str(), nullptr);
        WiredTigerBench::error_check(_INFORMCOSession->create(_INFORMCOSession, title.c_str(), userInformconfig.c_str()),
                                     "ColumnStoreResolver::loadData create userinformcol colgroup");
    }

    string userBehaviorColumnNames = "ID,UID,LOG_TIME,IP,DEVICE";
    string userBehaviorColgroups = userBehaviorColumnNames;
    boost::algorithm::to_lower(userBehaviorColgroups);
    vector<string> userBehaviorColumnList;
    vector<string> userBehaviorColgroupList;
    boost::algorithm::split(userBehaviorColumnList, userBehaviorColumnNames, boost::is_any_of( "," ));
    boost::algorithm::split(userBehaviorColgroupList, userBehaviorColgroups, boost::is_any_of( "," ));
    string userBehaviorconfig = "key_format=r,value_format=IISSS,columns=(MYID," + userBehaviorColumnNames + "),colgroups=(" + userBehaviorColgroups + ")";
    WiredTigerBench::error_check(_BEHAVIORCOSession->create(_BEHAVIORCOSession, "table:userbehaviorcol", userBehaviorconfig.c_str()),
                                 "ColumnStoreResolver::loadData create userbehaviorcol");
//    _BEHAVIORCOSession->drop(_BEHAVIORCOSession, "table:userbehaviorcol", nullptr);
    for (int i = 0; i < userBehaviorColumnList.size(); i++) {
        string title = "colgroup:userbehaviorcol:" + userBehaviorColgroupList[i];
        userBehaviorconfig = "columns=(" + userBehaviorColumnList[i] + ")";
//        _BEHAVIORCOSession->drop(_BEHAVIORCOSession, title.c_str(), nullptr);
        WiredTigerBench::error_check(_BEHAVIORCOSession->create(_BEHAVIORCOSession, title.c_str(), userBehaviorconfig.c_str()),
                                     "ColumnStoreResolver::loadData create userbehaviorcol colgroup");
    }

    cout << "execute column store test..." << endl;
}

void UserColumnStoreResolver::loadData(unsigned long preLoad1, unsigned long actualLoad1, unsigned long preLoad2, unsigned long actualLoad2) {
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
    WiredTigerBench::error_check(_INFORMCOSession->open_cursor(_INFORMCOSession, "table:userinformcol", nullptr, "append", &_INFORMcursor),
                                 "UserColumnStoreResolver::loadData 1");
    cout << "preLoading " << preLoad1 << " rows" << endl;
    cout << "insert for " << actualLoad1 << " rows" << endl;
    auto startTime1 = chrono::system_clock::now();
    for (unsigned long i = preLoad1; i < preLoad1 + actualLoad1; i++) {
        UserInformStruct* item = UserInformSource::parseUserInformStruct(lines1[i]);
        _INFORMcursor->set_value(_INFORMcursor, item->id, item->name, item->age, item->gender, item->occupation, item->register_date);
        WiredTigerBench::error_check(_INFORMcursor->insert(_INFORMcursor), "UserColumnStoreResolver::loadData 3");
        delete item;
    }
    auto endTime1 = chrono::system_clock::now();
    auto duration1 = chrono::duration_cast<chrono::seconds>(endTime1- startTime1);
    cout << "***insert user_inform time:" << duration1.count() << "s" << endl;
    WiredTigerBench::error_check(_INFORMcursor->close(_INFORMcursor), "UserColumnStoreResolver::loadData 4");
    _totalRows = preLoad1 + actualLoad1;

    WT_CURSOR* _BEHAVIORcursor = nullptr;
    WiredTigerBench::error_check(_BEHAVIORCOSession->open_cursor(_BEHAVIORCOSession, "table:userbehaviorcol", nullptr, "append", &_BEHAVIORcursor),
                                 "UserColumnStoreResolver::loadData 1");
    cout << "preLoading " << preLoad2 << " rows" << endl;
    cout << "insert for " << actualLoad2 << " rows" << endl;
    auto startTime2 = chrono::system_clock::now();
    for (unsigned long i = preLoad2; i < preLoad2 + actualLoad2; i++) {
        UserBehaviorStruct* item = UserBehaviorSource::parseUserBehaviorStruct(lines2[i]);
        _BEHAVIORcursor->set_value(_BEHAVIORcursor, item->id, item->uid, item->logTime, item->ip, item->device);
        WiredTigerBench::error_check(_BEHAVIORcursor->insert(_BEHAVIORcursor), "UserColumnStoreResolver::loadData 3");
        delete item;
    }
    auto endTime2 = chrono::system_clock::now();
    auto duration2 = chrono::duration_cast<chrono::seconds>(endTime2 - startTime2);
    cout << "***insert user_behavior time:" << duration2.count() << "s" << endl;
    WiredTigerBench::error_check(_BEHAVIORcursor->close(_BEHAVIORcursor), "UserColumnStoreResolver::loadData 4");
    _totalRows = preLoad2 + actualLoad2;
}

UserColumnStoreResolver::~UserColumnStoreResolver() {
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

void UserColumnStoreResolver::createIndex() {
    // do nothing
}

void UserColumnStoreResolver::execQ1() {
    vector<uint32_t> uids;
    uids.emplace_back(500);
    uids.emplace_back(501);
    uids.emplace_back(502);
    uids.emplace_back(503);
    uids.emplace_back(504);
    WT_CURSOR* _behavior_uid_cursor = nullptr;
    WiredTigerBench::error_check(_BEHAVIORCOSession->open_cursor(_BEHAVIORCOSession, "colgroup:userbehaviorcol:uid", nullptr, "overwrite=1", &_behavior_uid_cursor),
                                 "Q1:1");
    auto startTime = chrono::system_clock::now();
    int ret;
    vector<int> myids1;
    for(int i = 0; i < uids.size(); i++){
        _behavior_uid_cursor->reset(_behavior_uid_cursor);
        int myid;
        int uid;
        while((ret = _behavior_uid_cursor->next(_behavior_uid_cursor)) == 0){
            WiredTigerBench::error_check(_behavior_uid_cursor->get_value(_behavior_uid_cursor, &uid), "Q1:2");
            if(uid == uids[i]){
                WiredTigerBench::error_check(_behavior_uid_cursor->get_key(_behavior_uid_cursor, &myid), "Q1:3");
                myids1.emplace_back(myid);
            }
        }
    }
    WT_CURSOR* _behavior_log_time_cursor = nullptr;
    WiredTigerBench::error_check(_BEHAVIORCOSession->open_cursor(_BEHAVIORCOSession, "colgroup:userbehaviorcol:log_time", nullptr, "overwrite=1", &_behavior_log_time_cursor),
                                 "Q1:4");
    vector<int> myids2;
    for(int i = 0; i < myids1.size(); i++){
        _behavior_log_time_cursor->reset(_behavior_log_time_cursor);
        _behavior_log_time_cursor->set_key(_behavior_log_time_cursor, myids1[i]);
        WiredTigerBench::error_check(_behavior_log_time_cursor->search(_behavior_log_time_cursor), "Q1:5");
        char* log_time;
        WiredTigerBench::error_check(_behavior_log_time_cursor->get_value(_behavior_log_time_cursor, &log_time), "Q1:6");
        if(stoi(log_time) >= 20200719 && stoi(log_time) <= 20200726){
            myids2.emplace_back(myids1[i]);
        }
    }
    WT_CURSOR* _behavior_device_cursor = nullptr;
    WiredTigerBench::error_check(_BEHAVIORCOSession->open_cursor(_BEHAVIORCOSession, "colgroup:userbehaviorcol:device", nullptr, "overwrite=1", &_behavior_device_cursor),
                                 "Q1:7");
    for(int i = 0; i < myids2.size(); i++){
        _behavior_device_cursor->reset(_behavior_device_cursor);
        _behavior_device_cursor->set_key(_behavior_device_cursor, myids2[i]);
        WiredTigerBench::error_check(_behavior_device_cursor->search(_behavior_device_cursor), "Q1:8");
        char* device;
        WiredTigerBench::error_check(_behavior_device_cursor->get_value(_behavior_device_cursor, &device), "Q1:9");
        if(print){
            cout << device << endl;
        }
    }
    auto endTime = chrono::system_clock::now();
    auto duration = chrono::duration_cast<chrono::milliseconds>(endTime - startTime);
    _behavior_uid_cursor->close(_behavior_uid_cursor);
    _behavior_log_time_cursor->close(_behavior_log_time_cursor);
    _behavior_device_cursor->close(_behavior_device_cursor);
    cout << "Q1 Behavior RowStore use Time:" << duration.count() << "ms" << endl;
}

void UserColumnStoreResolver::execQ2() {
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
    WiredTigerBench::error_check(_INFORMCOSession->open_cursor(_INFORMCOSession, "colgroup:userinformcol:id", nullptr, "overwrite=1", &_inform_id_cursor),
                                 "Q2:1");
    WT_CURSOR * _inform_register_date_cursor;
    WiredTigerBench::error_check(_INFORMCOSession->open_cursor(_INFORMCOSession, "colgroup:userinformcol:register_date", nullptr, "overwrite=1", &_inform_register_date_cursor),
                                 "Q2:2");
    WT_CURSOR * _behavior_uid_cursor;
    WiredTigerBench::error_check(_BEHAVIORCOSession->open_cursor(_BEHAVIORCOSession, "colgroup:userbehaviorcol:uid", nullptr, "overwrite=1", &_behavior_uid_cursor),
                                 "Q2:3");
    WT_CURSOR * _behavior_log_time_cursor;
    WiredTigerBench::error_check(_BEHAVIORCOSession->open_cursor(_BEHAVIORCOSession, "colgroup:userbehaviorcol:log_time", nullptr, "overwrite=1", &_behavior_log_time_cursor),
                                 "Q2:4");
    int ret;
    for(int i = 0; i < answer_ids.size(); i++){
        auto startTime1 = chrono::system_clock::now();
        _inform_id_cursor->reset(_inform_id_cursor);
        while((ret = _inform_id_cursor->next(_inform_id_cursor)) == 0){
            int myid;
            int id;
            WiredTigerBench::error_check(_inform_id_cursor->get_value(_inform_id_cursor, &id), "Q2:5");
            if(id == answer_ids[i]){
                WiredTigerBench::error_check(_inform_id_cursor->get_key(_inform_id_cursor, &myid), "Q2:6");
                _inform_register_date_cursor->reset(_inform_register_date_cursor);
                _inform_register_date_cursor->set_key(_inform_register_date_cursor, myid);
                _inform_register_date_cursor->search(_inform_register_date_cursor);
                char* register_date;
                WiredTigerBench::error_check(_inform_register_date_cursor->get_value(_inform_register_date_cursor, &register_date), "Q2:7");
                auto endTime1 = chrono::system_clock::now();
                duration1 = chrono::duration_cast<chrono::milliseconds>(endTime1 - startTime1 + duration1);
                auto startTime2 = chrono::system_clock::now();
                if(stoi(register_date) < 20120101){
                    _behavior_uid_cursor->reset(_behavior_uid_cursor);
                    while ((ret = _behavior_uid_cursor->next(_behavior_uid_cursor)) == 0){
                        int uid;
                        int myid2;
                        WiredTigerBench::error_check(_behavior_uid_cursor->get_value(_behavior_uid_cursor, &uid), "Q2:8");
                        if(uid == answer_ids[i]){
                            WiredTigerBench::error_check(_behavior_uid_cursor->get_key(_behavior_uid_cursor, &myid2), "Q2:9");
                            _behavior_log_time_cursor->reset(_behavior_log_time_cursor);
                            _behavior_log_time_cursor->set_key(_behavior_log_time_cursor, myid2);
                            _behavior_log_time_cursor->search(_behavior_log_time_cursor);
                            char* log_time;
                            WiredTigerBench::error_check(_behavior_log_time_cursor->get_value(_behavior_log_time_cursor, &log_time), "Q2:10");
                            if(stoi(log_time) == 20200725 && print){
                                cout << answer_ids[i] << endl;
                            }
                        }
                    }
                }
                auto endTime2 = chrono::system_clock::now();
                duration2 = chrono::duration_cast<chrono::milliseconds>(endTime2 - startTime2 + duration2);
            }
        }
    }
    auto endTime = chrono::system_clock::now();
    auto duration = chrono::duration_cast<chrono::milliseconds>(endTime - startTime);
    _inform_id_cursor->close(_inform_id_cursor);
    _inform_register_date_cursor->close(_inform_register_date_cursor);
    _behavior_uid_cursor->close(_behavior_uid_cursor);
    _behavior_log_time_cursor->close(_behavior_log_time_cursor);
    cout << "Q2 Inform ColumnStore use Time:" << duration1.count() << "ms" << endl;
    cout << "Q2 Behavior ColumnStore use Time:" << duration2.count() << "ms" << endl;
}

void UserColumnStoreResolver::execQ3() {
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
    WiredTigerBench::error_check(_INFORMCOSession->open_cursor(_INFORMCOSession, "colgroup:userinformcol:id", nullptr, "overwrite=1", &_inform_id_cursor),
                                 "Q3:1");
    WT_CURSOR * _inform_register_date_cursor;
    WiredTigerBench::error_check(_INFORMCOSession->open_cursor(_INFORMCOSession, "colgroup:userinformcol:register_date", nullptr, "overwrite=1", &_inform_register_date_cursor),
                                 "Q3:2");
    WT_CURSOR * _behavior_uid_cursor;
    WiredTigerBench::error_check(_BEHAVIORCOSession->open_cursor(_BEHAVIORCOSession, "colgroup:userbehaviorcol:uid", nullptr, "overwrite=1", &_behavior_uid_cursor),
                                 "Q3:3");
    WT_CURSOR * _behavior_log_time_cursor;
    WiredTigerBench::error_check(_BEHAVIORCOSession->open_cursor(_BEHAVIORCOSession, "colgroup:userbehaviorcol:log_time", nullptr, "overwrite=1", &_behavior_log_time_cursor),
                                 "Q3:4");
    int ret;
    for(int i = 0; i < answer_ids.size(); i++){
        auto startTime1 = chrono::system_clock::now();
        _inform_id_cursor->reset(_inform_id_cursor);
        while((ret = _inform_id_cursor->next(_inform_id_cursor)) == 0){
            int myid;
            int id;
            WiredTigerBench::error_check(_inform_id_cursor->get_value(_inform_id_cursor, &id), "Q3:5");
            if(id == answer_ids[i]){
                WiredTigerBench::error_check(_inform_id_cursor->get_key(_inform_id_cursor, &myid), "Q3:6");
                _inform_register_date_cursor->reset(_inform_register_date_cursor);
                _inform_register_date_cursor->set_key(_inform_register_date_cursor, myid);
                _inform_register_date_cursor->search(_inform_register_date_cursor);
                char* register_date;
                WiredTigerBench::error_check(_inform_register_date_cursor->get_value(_inform_register_date_cursor, &register_date), "Q3:7");
                auto endTime1 = chrono::system_clock::now();
                duration1 = chrono::duration_cast<chrono::milliseconds>(endTime1 - startTime1 + duration1);
                auto startTime2 = chrono::system_clock::now();
                if(stoi(register_date) < 20120101){
                    _behavior_uid_cursor->reset(_behavior_uid_cursor);
                    while ((ret = _behavior_uid_cursor->next(_behavior_uid_cursor)) == 0){
                        int uid;
                        int myid2;
                        WiredTigerBench::error_check(_behavior_uid_cursor->get_value(_behavior_uid_cursor, &uid), "Q3:8");
                        if(uid == answer_ids[i]){
                            WiredTigerBench::error_check(_behavior_uid_cursor->get_key(_behavior_uid_cursor, &myid2), "Q3:9");
                            _behavior_log_time_cursor->reset(_behavior_log_time_cursor);
                            _behavior_log_time_cursor->set_key(_behavior_log_time_cursor, myid2);
                            _behavior_log_time_cursor->search(_behavior_log_time_cursor);
                            char* log_time;
                            WiredTigerBench::error_check(_behavior_log_time_cursor->get_value(_behavior_log_time_cursor, &log_time), "Q3:10");
                            if(stoi(log_time) == 20200718 && print){
                                cout << answer_ids[i] << endl;
                            }
                        }
                    }
                }
                auto endTime2 = chrono::system_clock::now();
                duration2 = chrono::duration_cast<chrono::milliseconds>(endTime2 - startTime2 + duration2);
            }
        }
    }
    auto endTime = chrono::system_clock::now();
    auto duration = chrono::duration_cast<chrono::milliseconds>(endTime - startTime);
    _inform_id_cursor->close(_inform_id_cursor);
    _inform_register_date_cursor->close(_inform_register_date_cursor);
    _behavior_uid_cursor->close(_behavior_uid_cursor);
    _behavior_log_time_cursor->close(_behavior_log_time_cursor);
    cout << "Q3 Inform ColumnStore use Time:" << duration1.count() << "ms" << endl;
    cout << "Q3 Behavior ColumnStore use Time:" << duration2.count() << "ms" << endl;
}

void UserColumnStoreResolver::execQ4(){
    vector<int> answer_ids;
    answer_ids.emplace_back(500);
    answer_ids.emplace_back(501);
    answer_ids.emplace_back(502);
    answer_ids.emplace_back(503);
    answer_ids.emplace_back(504);
    auto startTime = chrono::system_clock::now();
    WT_CURSOR * _inform_id_cursor;
    WiredTigerBench::error_check(_INFORMCOSession->open_cursor(_INFORMCOSession, "colgroup:userinformcol:id", nullptr, "overwrite=1", &_inform_id_cursor),
                                 "Q4:1");
    WT_CURSOR * _inform_name_cursor;
    WiredTigerBench::error_check(_INFORMCOSession->open_cursor(_INFORMCOSession, "colgroup:userinformcol:name", nullptr, "overwrite=1", &_inform_name_cursor),
                                 "Q4:2");
    WT_CURSOR * _inform_age_cursor;
    WiredTigerBench::error_check(_INFORMCOSession->open_cursor(_INFORMCOSession, "colgroup:userinformcol:age", nullptr, "overwrite=1", &_inform_age_cursor),
                                 "Q4:3");
    WT_CURSOR * _inform_gender_cursor;
    WiredTigerBench::error_check(_INFORMCOSession->open_cursor(_INFORMCOSession, "colgroup:userinformcol:gender", nullptr, "overwrite=1", &_inform_gender_cursor),
                                 "Q4:4");
    WT_CURSOR * _inform_occupation_cursor;
    WiredTigerBench::error_check(_INFORMCOSession->open_cursor(_INFORMCOSession, "colgroup:userinformcol:occupation", nullptr, "overwrite=1", &_inform_occupation_cursor),
                                 "Q4:5");
    WT_CURSOR * _inform_register_date_cursor;
    WiredTigerBench::error_check(_INFORMCOSession->open_cursor(_INFORMCOSession, "colgroup:userinformcol:register_date", nullptr, "overwrite=1", &_inform_register_date_cursor),
                                 "Q4:6");
    int ret;
    for(int i = 0;i < answer_ids.size(); i++){
        _inform_id_cursor->reset(_inform_id_cursor);
        while((ret = _inform_id_cursor->next(_inform_id_cursor)) == 0){
            int myid;
            int id;
            WiredTigerBench::error_check(_inform_id_cursor->get_value(_inform_id_cursor, &id), "Q4:7");
            if(id == answer_ids[i]){
                WiredTigerBench::error_check(_inform_id_cursor->get_key(_inform_id_cursor, &myid), "Q4:8");
                _inform_register_date_cursor->reset(_inform_register_date_cursor);
                char* register_date;
                _inform_register_date_cursor->set_key(_inform_register_date_cursor, myid);
                WiredTigerBench::error_check(_inform_register_date_cursor->search(_inform_register_date_cursor), "Q4:9");
                WiredTigerBench::error_check(_inform_register_date_cursor->get_value(_inform_register_date_cursor, &register_date), "Q4:10");
                if(stoi(register_date) < 20100101){
                    _inform_name_cursor->reset(_inform_name_cursor);
                    _inform_name_cursor->set_key(_inform_name_cursor, myid);
                    _inform_name_cursor->search(_inform_name_cursor);
                    char* name;
                    _inform_name_cursor->get_value(_inform_name_cursor, &name);

                    _inform_age_cursor->reset(_inform_age_cursor);
                    _inform_age_cursor->set_key(_inform_age_cursor, myid);
                    _inform_age_cursor->search(_inform_age_cursor);
                    int age;
                    _inform_age_cursor->get_value(_inform_age_cursor, &age);

                    _inform_gender_cursor->reset(_inform_gender_cursor);
                    _inform_gender_cursor->set_key(_inform_gender_cursor, myid);
                    _inform_gender_cursor->search(_inform_gender_cursor);
                    char* tmp;
                    _inform_gender_cursor->get_value(_inform_gender_cursor, &tmp);
                    char gender = tmp[0];

                    _inform_occupation_cursor->reset(_inform_occupation_cursor);
                    _inform_occupation_cursor->set_key(_inform_occupation_cursor, myid);
                    _inform_occupation_cursor->search(_inform_occupation_cursor);
                    char* occupation;
                    _inform_occupation_cursor->get_value(_inform_occupation_cursor, &occupation);
                    if(print){
                        cout << answer_ids[i] << " " << name << " " << age << " " << gender << " " << occupation << " " << register_date << endl;
                    }
                }
            }
        }
    }
    auto endTime = chrono::system_clock::now();
    auto duration = chrono::duration_cast<chrono::milliseconds>(endTime - startTime);
    _inform_id_cursor->close(_inform_id_cursor);
    _inform_name_cursor->close(_inform_name_cursor);
    _inform_age_cursor->close(_inform_age_cursor);
    _inform_gender_cursor->close(_inform_gender_cursor);
    _inform_occupation_cursor->close(_inform_occupation_cursor);
    _inform_register_date_cursor->close(_inform_register_date_cursor);
    cout << "Q4 Inform RowStore use Time:" << duration.count() << "ms" << endl;
}

void UserColumnStoreResolver::execQ5(){
    vector<int> answer_ids;
    answer_ids.emplace_back(500);
    answer_ids.emplace_back(501);
    answer_ids.emplace_back(502);
    answer_ids.emplace_back(503);
    answer_ids.emplace_back(504);
    auto startTime = chrono::system_clock::now();
    WT_CURSOR * _inform_id_cursor;
    WiredTigerBench::error_check(_INFORMCOSession->open_cursor(_INFORMCOSession, "colgroup:userinformcol:id", nullptr, "overwrite=1", &_inform_id_cursor),
                                 "Q5:1");
    WT_CURSOR * _inform_age_cursor;
    WiredTigerBench::error_check(_INFORMCOSession->open_cursor(_INFORMCOSession, "colgroup:userinformcol:age", nullptr, "overwrite=1", &_inform_age_cursor),
                                 "Q5:2");
    int ret;
    for(int i = 0; i < answer_ids.size(); i++){
        _inform_id_cursor->reset(_inform_id_cursor);
        while((ret = _inform_id_cursor->next(_inform_id_cursor)) == 0){
            int myid;
            int id;
            WiredTigerBench::error_check(_inform_id_cursor->get_value(_inform_id_cursor, &id), "Q5:3");
            if(id == answer_ids[i]){
                WiredTigerBench::error_check(_inform_id_cursor->get_key(_inform_id_cursor, &myid), "Q5:4");
                _inform_age_cursor->reset(_inform_age_cursor);
                _inform_age_cursor->set_key(_inform_age_cursor, myid);
                _inform_age_cursor->search(_inform_age_cursor);
                int age;
                _inform_age_cursor->get_value(_inform_age_cursor, &age);
                if(age >= 20 && age <= 30 && print){
                    cout << answer_ids[i] << endl;
                }
            }
        }
    }
    auto endTime = chrono::system_clock::now();
    auto duration = chrono::duration_cast<chrono::milliseconds>(endTime - startTime);
    cout << "Q5 Inform RowStore use Time:" << duration.count() << "ms" << endl;
}
