#include <sstream>
#include <iostream>
#include <fstream>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string.hpp>
#include "UserLSMGroupResolver.h"
#include "../dataDesc.h"
#include "../UserInform.h"
#include "../UserInformSource.h"
#include "../UserBehavior.h"
#include "../UserBehaviorSource.h"

//
// Created by yanhao on 2020/8/10.
//
UserLSMGroupResolver::UserLSMGroupResolver(string &informTablePath, string &behaviorTablePath, string &userInformPath,
                                           string &userBehaviorPath, string &workDir) : UserBaseResolver(
        informTablePath, behaviorTablePath, userInformPath,
        userBehaviorPath, workDir) {
    stringstream _inform_ss;
    _inform_ss << "%0" << ID1_LEN << "d%";
    _inform_keyFormat = _inform_ss.str();

    stringstream _behavior_ss;
    _behavior_ss << "%0" << ID2_LEN << "d%";
    _behavior_keyFormat = _behavior_ss.str();

    rocksdb::Options _inform_options;
    _inform_options.IncreaseParallelism();
    _inform_options.OptimizeLevelStyleCompaction();
    _inform_options.create_if_missing = true;

//    std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
//    column_families.push_back(rocksdb::ColumnFamilyDescriptor( "main", rocksdb::ColumnFamilyOptions()));
//    column_families.push_back(rocksdb::ColumnFamilyDescriptor( "other", rocksdb::ColumnFamilyOptions()));
//    std::vector<rocksdb::ColumnFamilyHandle*> handles;
//    handles.push_back(_inform_mainGroup);
//    handles.push_back(_inform_otherGroup);
    rocksdb::Status _inform_status = rocksdb::DB::Open(_inform_options, informTablePath, &_inform_db);
    if(!_inform_status.ok()){
        cout << _inform_status.ToString() << endl;
    }
//    _inform_db->DropColumnFamily(_inform_mainGroup);
//    _inform_db->DropColumnFamily(_inform_otherGroup);
//    _inform_status = _inform_db->DestroyColumnFamilyHandle(_inform_mainGroup);
//    _inform_status = _inform_db->DestroyColumnFamilyHandle(_inform_otherGroup);

    _inform_status = _inform_db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), "main", &_inform_mainGroup);
    assert(_inform_status.ok());
    _inform_status = _inform_db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), "other", &_inform_otherGroup);
    assert(_inform_status.ok());

    rocksdb::Options _behavior_options;
    _behavior_options.IncreaseParallelism();
    _behavior_options.OptimizeLevelStyleCompaction();
    _behavior_options.create_if_missing = true;
//    std::vector<rocksdb::ColumnFamilyDescriptor> column_families2;
//    column_families2.push_back(rocksdb::ColumnFamilyDescriptor( "main", rocksdb::ColumnFamilyOptions()));
//    column_families2.push_back(rocksdb::ColumnFamilyDescriptor( "other", rocksdb::ColumnFamilyOptions()));
//    std::vector<rocksdb::ColumnFamilyHandle*> handles2;
//    handles2.push_back(_behavior_mainGroup);
//    handles2.push_back(_behavior_otherGroup);
    rocksdb::Status _behavior_status = rocksdb::DB::Open(_behavior_options, behaviorTablePath, &_behavior_db);
    if(!_behavior_status.ok()){
        cout << _behavior_status.ToString() << endl;
    }
//    _behavior_db->DropColumnFamily(_behavior_mainGroup);
//    _behavior_db->DropColumnFamily(_behavior_otherGroup);
//    _behavior_status = _behavior_db->DestroyColumnFamilyHandle(_behavior_mainGroup);
//    _behavior_status = _behavior_db->DestroyColumnFamilyHandle(_behavior_otherGroup);

    _behavior_status = _behavior_db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), "main", &_behavior_mainGroup);
    assert(_behavior_status.ok());
    _behavior_status = _behavior_db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), "other", &_behavior_otherGroup);
    assert(_behavior_status.ok());

    cout << "execute lsm (2 colgroups) test..." << endl;
}

void UserLSMGroupResolver::loadData(unsigned long preLoad1, unsigned long actualLoad1, unsigned long preLoad2,
                                   unsigned long actualLoad2) {
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

    cout << "preLoading " << preLoad1 << " rows" << endl;
    for (unsigned long i = 0; i < preLoad1; i++) {
        UserInformStruct* item = UserInformSource::parseUserInformStruct(lines1[i]);
        char leadKeyBuf[USERINFORM_KEY_LEN];
        memset(leadKeyBuf, 0, USERINFORM_KEY_LEN);
        sprintf(leadKeyBuf, _inform_keyFormat.data(), item->id);
        string key(leadKeyBuf);
        stringstream value1;
        value1 << item->register_date;
        rocksdb::Status s = _inform_db->Put(_inform_writeOptions, _inform_mainGroup, key, value1.str());
        assert(s.ok());
        stringstream value2;
        value2 << item->name << "|" << item->age << "|" << item->gender << "|" << item->occupation;
        s = _inform_db->Put(_inform_writeOptions, _inform_otherGroup, key, value2.str());
        assert(s.ok());
        delete item;
    }
    cout << "insert for " << actualLoad1 << " rows" << endl;
    auto startTime1 = chrono::system_clock::now();
    for (unsigned long i = preLoad1; i < preLoad1 + actualLoad1; i++) {
        UserInformStruct* item = UserInformSource::parseUserInformStruct(lines1[i]);
        char leadKeyBuf[USERINFORM_KEY_LEN];
        memset(leadKeyBuf, 0, USERINFORM_KEY_LEN);
        sprintf(leadKeyBuf, _inform_keyFormat.data(), item->id);
        string key(leadKeyBuf);
        stringstream value1;
        value1 << item->register_date;
        rocksdb::Status s = _inform_db->Put(_inform_writeOptions, _inform_mainGroup, key, value1.str());
        assert(s.ok());
        stringstream value2;
        value2 << item->name << "|" << item->age << "|" << item->gender << "|" << item->occupation;
        s = _inform_db->Put(_inform_writeOptions, _inform_otherGroup, key, value2.str());
        assert(s.ok());
        delete item;
    }
    auto endTime1 = chrono::system_clock::now();
    auto duration1 = chrono::duration_cast<chrono::seconds>(endTime1 - startTime1);
    cout << "***insert userinform time:" << duration1.count() << "s" << endl;
    _totalRows = preLoad1 + actualLoad1;

    cout << "preLoading " << preLoad2 << " rows" << endl;
    for (unsigned long i = 0; i < preLoad2; i++) {
        UserBehaviorStruct* item = UserBehaviorSource::parseUserBehaviorStruct(lines2[i]);
        char leadKeyBuf[USERBEHAVIOR_KEY_LEN];
        memset(leadKeyBuf, 0, USERBEHAVIOR_KEY_LEN);
        sprintf(leadKeyBuf, _behavior_keyFormat.data(), item->id);

        string key(leadKeyBuf);
        stringstream value1;
        value1 << item->uid << "|" << item->logTime << "|" << item->device;
        rocksdb::Status s = _behavior_db->Put(_behavior_writeOptions, _behavior_mainGroup, key, value1.str());
        assert(s.ok());
        stringstream value2;
        value2 << item->ip;
        s = _behavior_db->Put(_behavior_writeOptions, _behavior_otherGroup, key, value2.str());
        assert(s.ok());
        delete item;
    }
    cout << "insert for " << actualLoad2 << " rows" << endl;
    auto startTime2 = chrono::system_clock::now();
    for (unsigned long i = preLoad2; i < preLoad2 + actualLoad2; i++) {
        UserBehaviorStruct* item = UserBehaviorSource::parseUserBehaviorStruct(lines2[i]);
        char leadKeyBuf[USERBEHAVIOR_KEY_LEN];
        memset(leadKeyBuf, 0, USERBEHAVIOR_KEY_LEN);
        sprintf(leadKeyBuf, _behavior_keyFormat.data(), item->id);

        string key(leadKeyBuf);
        stringstream value1;
        value1 << item->uid << "|" << item->logTime << "|" << item->device;
        rocksdb::Status s = _behavior_db->Put(_behavior_writeOptions, _behavior_mainGroup, key, value1.str());
        assert(s.ok());
        stringstream value2;
        value2 << item->ip;
        s = _behavior_db->Put(_behavior_writeOptions, _behavior_otherGroup, key, value2.str());
        assert(s.ok());
        delete item;
    }
    auto endTime2 = chrono::system_clock::now();
    auto duration2 = chrono::duration_cast<chrono::seconds>(endTime2 - startTime2);
    cout << "***insert userbehavior time:" << duration2.count() << "s" << endl;
    _totalRows = preLoad2 + actualLoad2;
}

UserLSMGroupResolver::~UserLSMGroupResolver() {
    _inform_db->DropColumnFamily(_inform_mainGroup);
    _inform_db->DropColumnFamily(_inform_otherGroup);
    _inform_db->DestroyColumnFamilyHandle(_inform_mainGroup);
    _inform_db->DestroyColumnFamilyHandle(_inform_otherGroup);
    delete _inform_db;

    _behavior_db->DropColumnFamily(_behavior_mainGroup);
    _behavior_db->DropColumnFamily(_behavior_otherGroup);
    _behavior_db->DestroyColumnFamilyHandle(_behavior_mainGroup);
    _behavior_db->DestroyColumnFamilyHandle(_behavior_otherGroup);
    delete _behavior_db;
}

void UserLSMGroupResolver::createIndex() {

}

void UserLSMGroupResolver::execQ1() {
    vector<int> answer_ids;
    answer_ids.emplace_back(500);
    answer_ids.emplace_back(501);
    answer_ids.emplace_back(502);
    answer_ids.emplace_back(503);
    answer_ids.emplace_back(504);
    auto startTime = chrono::system_clock::now();
    for (int i = 0; i < answer_ids.size(); i++) {
        char id[ID2_LEN];
        sprintf(id, _behavior_keyFormat.data(), 0);
        string target = string(id);
        rocksdb::Iterator *_behavior_it = _behavior_db->NewIterator(rocksdb::ReadOptions(), _behavior_mainGroup);
        _behavior_it->Seek(target);
        while (_behavior_it->Valid()) {
            string keyString = _behavior_it->key().ToString();
            string valueString = _behavior_it->value().ToString();
            vector<string> valueList;
            boost ::algorithm::split(valueList, valueString, boost::is_any_of("|"));
            if (stoi(valueList[0]) == answer_ids[i] && stoi(valueList[1]) >= 20200719 && stoi(valueList[1]) <= 20200726 &&
                print) {
                cout << valueList[2] << endl;
            }
            _behavior_it->Next();
        }
        delete _behavior_it;
    }
    auto endTime = chrono::system_clock::now();
    auto duration = chrono::duration_cast<chrono::milliseconds>(endTime - startTime);
    cout << "Q1 Behavior LSMGroup use Time:" << duration.count() << "ms" << endl;
}

void UserLSMGroupResolver::execQ2() {
    vector<int> answer_ids;
    answer_ids.emplace_back(500);
    answer_ids.emplace_back(501);
    answer_ids.emplace_back(502);
    answer_ids.emplace_back(503);
    answer_ids.emplace_back(504);
    auto startTime = chrono::system_clock::now();
    auto duration1 = chrono::duration_cast<chrono::milliseconds>(startTime - startTime);
    auto duration2 = chrono::duration_cast<chrono::milliseconds>(startTime - startTime);
    for (int i = 0; i < answer_ids.size(); i++) {
        auto startTime1 = chrono::system_clock::now();
        char id[ID1_LEN];
        sprintf(id, _inform_keyFormat.data(), answer_ids[i]);
        string _inform_target = string(id);
        rocksdb::Iterator *_inform_it = _inform_db->NewIterator(rocksdb::ReadOptions(), _inform_mainGroup);
        _inform_it->Seek(_inform_target);
        if (_inform_it->Valid()) {
            string valueString = _inform_it->value().ToString();
            vector<string> valueList;
            boost::algorithm::split(valueList, valueString, boost::is_any_of("|"));
            auto endTime1 = chrono::system_clock::now();
            duration1 = chrono::duration_cast<chrono::milliseconds>(endTime1 - startTime1 + duration1);

            auto startTime2 = chrono::system_clock::now();
            if (stoi(valueList[0]) < 20120101) {
                char id2[ID2_LEN];
                sprintf(id2, _behavior_keyFormat.data(), 0);
                string _behavior_target = string(id2);
                rocksdb::Iterator *_behavior_it = _behavior_db->NewIterator(rocksdb::ReadOptions(), _behavior_mainGroup);
                _behavior_it->Seek(_behavior_target);
                while (_behavior_it->Valid()) {
                    string valueString2 = _behavior_it->value().ToString();
                    vector<string> valueList2;
                    boost::algorithm::split(valueList2, valueString2, boost::is_any_of("|"));
                    if (stoi(valueList2[1]) == 20200725 && print) {
                        cout << answer_ids[i] << endl;
                    }
                    _behavior_it->Next();
                }
                delete _behavior_it;
            }
            auto endTime2 = chrono::system_clock::now();
            duration2 = chrono::duration_cast<chrono::milliseconds>(endTime2 - startTime2 + duration2);
        }
        delete _inform_it;
    }
    auto endTime = chrono::system_clock::now();
    auto duration = chrono::duration_cast<chrono::milliseconds>(endTime - startTime);
    cout << "Q2 Inform LSMGroup  use Time:" << duration1.count() << "ms" << endl;
    cout << "Q2 Behavior LSMGroup use Time:" << duration2.count() << "ms" << endl;
}

void UserLSMGroupResolver::execQ3() {
    vector<int> answer_ids;
    answer_ids.emplace_back(500);
    answer_ids.emplace_back(501);
    answer_ids.emplace_back(502);
    answer_ids.emplace_back(503);
    answer_ids.emplace_back(504);
    auto startTime = chrono::system_clock::now();
    auto duration1 = chrono::duration_cast<chrono::milliseconds>(startTime - startTime);
    auto duration2 = chrono::duration_cast<chrono::milliseconds>(startTime - startTime);
    for (int i = 0; i < answer_ids.size(); i++) {
        auto startTime1 = chrono::system_clock::now();
        char id[ID1_LEN];
        sprintf(id, _inform_keyFormat.data(), answer_ids[i]);
        string _inform_target = string(id);
        rocksdb::Iterator *_inform_it = _inform_db->NewIterator(rocksdb::ReadOptions(), _inform_mainGroup);
        _inform_it->Seek(_inform_target);
        if (_inform_it->Valid()) {
            string valueString = _inform_it->value().ToString();
            vector<string> valueList;
            boost::algorithm::split(valueList, valueString, boost::is_any_of("|"));
            auto endTime1 = chrono::system_clock::now();
            duration1 = chrono::duration_cast<chrono::milliseconds>(endTime1 - startTime1 + duration1);

            auto startTime2 = chrono::system_clock::now();
            if (stoi(valueList[0]) < 20120101) {
                char id2[ID2_LEN];
                sprintf(id2, _behavior_keyFormat.data(), 0);
                string _behavior_target = string(id2);
                rocksdb::Iterator *_behavior_it = _behavior_db->NewIterator(rocksdb::ReadOptions(), _behavior_mainGroup);
                _behavior_it->Seek(_behavior_target);
                while (_behavior_it->Valid()) {
                    string valueString2 = _behavior_it->value().ToString();
                    vector<string> valueList2;
                    boost::algorithm::split(valueList2, valueString2, boost::is_any_of("|"));
                    if (stoi(valueList2[1]) == 20200718 && print) {
                        cout << answer_ids[i] << endl;
                    }
                    _behavior_it->Next();
                }
                delete _behavior_it;
            }
            auto endTime2 = chrono::system_clock::now();
            duration2 = chrono::duration_cast<chrono::milliseconds>(endTime2 - startTime2 + duration2);
        }
        delete _inform_it;
    }
    auto endTime = chrono::system_clock::now();
    auto duration = chrono::duration_cast<chrono::milliseconds>(endTime - startTime);
    cout << "Q3 Inform LSMGroup  use Time:" << duration1.count() << "ms" << endl;
    cout << "Q3 Behavior LSMGroup use Time:" << duration2.count() << "ms" << endl;
}

void UserLSMGroupResolver::execQ4() {
    vector<int> answer_ids;
    answer_ids.emplace_back(500);
    answer_ids.emplace_back(501);
    answer_ids.emplace_back(502);
    answer_ids.emplace_back(503);
    answer_ids.emplace_back(504);
    auto startTime = chrono::system_clock::now();
    for (int i = 0; i < answer_ids.size(); i++) {
        char id[ID1_LEN];
        sprintf(id, _inform_keyFormat.data(), answer_ids[i]);
        string _inform_target = string(id);
        rocksdb::Iterator *_inform_it = _inform_db->NewIterator(rocksdb::ReadOptions(), _inform_mainGroup);
        _inform_it->Seek(_inform_target);
        if (_inform_it->Valid()) {
            string valueString = _inform_it->value().ToString();
            vector<string> valueList;
            boost::algorithm::split(valueList, valueString, boost::is_any_of("|"));
            if (stoi(valueList[0]) < 20100101 && print) {
                rocksdb::Iterator *_inform_it2 = _inform_db->NewIterator(rocksdb::ReadOptions(), _inform_otherGroup);
                _inform_it2->Seek(_inform_target);
                if (_inform_it2->Valid()){
                    string valueString2 = _inform_it2->value().ToString();
                    vector<string> valueList2;
                    boost::algorithm::split(valueList2, valueString2, boost::is_any_of("|"));
                    cout << answer_ids[i] << " " << valueList2[0] << " " << int(valueList2[1][0]) << " " << valueList2[2] << " "
                         << valueList2[3] << " " << valueList[0] << endl;
                }
            }
        }
        delete _inform_it;
    }
    auto endTime = chrono::system_clock::now();
    auto duration = chrono::duration_cast<chrono::milliseconds>(endTime - startTime);
    cout << "Q4 Inform LSMGroup use Time:" << duration.count() << "ms" << endl;
}

void UserLSMGroupResolver::execQ5() {
    vector<int> answer_ids;
    answer_ids.emplace_back(500);
    answer_ids.emplace_back(501);
    answer_ids.emplace_back(502);
    answer_ids.emplace_back(503);
    answer_ids.emplace_back(504);
    auto startTime = chrono::system_clock::now();
    for (int i = 0; i < answer_ids.size(); i++) {
        char id[ID1_LEN];
        sprintf(id, _inform_keyFormat.data(), answer_ids[i]);
        string _inform_target = string(id);
        rocksdb::Iterator *_inform_it = _inform_db->NewIterator(rocksdb::ReadOptions(), _inform_otherGroup);
        _inform_it->Seek(_inform_target);
        if (_inform_it->Valid()) {
            string valueString = _inform_it->value().ToString();
            vector<string> valueList;
            boost::algorithm::split(valueList, valueString, boost::is_any_of("|"));
            if (int(valueList[1][0]) <= 30 && int(valueList[1][0]) >= 20 && print) {
                cout << answer_ids[i] << endl;
            }
        }
        delete _inform_it;
    }
    auto endTime = chrono::system_clock::now();
    auto duration = chrono::duration_cast<chrono::milliseconds>(endTime - startTime);
    cout << "Q5 Inform LSMGroup use Time:" << duration.count() << "ms" << endl;
}
