#include <iostream>
#include <string>
#include <cstring>
#include <fstream>
#include <thread>

#include "RocksdbBench.h"
#include "WiredTigerBench.h"
#include "rocksdb/options.h"
#include "tools/Histogram.h"
#include "tools/Tools.h"
#include "tools/Throughput.h"
#include "GenerateSampler.h"
#include "BenchmarkConfig.h"
#include "CombinedSource.h"
#include "LineItemQueryResolver.h"
#include "query/LSMFullResolver.h"
#include "query/RowStoreResolver.h"
#include "query/LSMGroupResolver.h"
#include "query/ColumnStoreResolver.h"
#include "query/UserBaseResolver.h"
#include "query/UserRowStoreResolver.h"
#include "query/UserColumnStoreResolver.h"
#include "query/UserLSMFullResolver.h"
#include "query/UserLSMGroupResolver.h"

void lineItemInfo(const char *lineItemFile);

using namespace std;

int main(int argc, char *argv[]) {

    cout << "current code version: 2020-08-14 9:00:00" << endl;
//    if (argc < 2) {
//        cout << "usage main seqnum" << endl;
//        return 0;
//    }
//
//    srandom(time(0));
//    srand(time(0));
//    GenerateSampler sampler;
//    string path = string("/media/yanhao/Windows/Users/YANHAO/Desktop/HITDB/ctest");
//    int i = stoi(argv[1]);
//    sampler.runColumnExp(path, SEQ, 2 * i + 1);
//    sampler.runColumnExp(path, RAND, 2 * i + 2);

//    GenerateSampler sampler;
//    string path = string("../userInformTest");
//    string userInformPath = string("../data/user_inform.csv");
//    sampler.runUserInformColumnExp(userInformPath, path, SEQ, 1001);

//    GenerateSampler sampler;
//    string path = string("/home/ironwei/colTest");
//    string randlineItemPath = string("/home/weiyan/lineItem/randlineitem.tbl");
    //sampler.runTPC(lineItemPath, path, SEQ, 1001);
//    sampler.runTPC(randlineItemPath, path, RAND, 1002);
//    sampler.runColumnExp(path, SEQ, 2 * i + 1);
//    sampler.runColumnExp(path, RAND, 2 * i + 2);
//    sampler.runColumnFCExp(path);
//    sampler.runTPCColumnExp(lineItemPath, path, SEQ, 1001);
//    string dataPath = "/home/weiyan/tpchdata";
    string tablePath = "/home/yanhao/桌面/HITDB/storeBenchmark/tbl";
//    string testPath = "/home/weiyan/litest";
    string lineItemPath = "/home/yanhao/桌面/HITDB/storeBenchmark/tpchData/lineitem.tbl";
//    LineItemQueryResolver resolver;
//    resolver.initTPCTable(dataPath, tablePath, false);
//    resolver.loadLineItem(lineItemPath, testPath, false,0,100);
//    vector<string> segments;
//    vector<int> dayOfMonths;
//    segments.emplace_back("BUILDING");
//    dayOfMonths.emplace_back(15);
//    resolver.batchExecQ3(segments, dayOfMonths);
//    vector<string> region;
//    vector<int> year;
//    region.emplace_back("ASIA");
//    year.emplace_back(1994);
//    resolver.batchExecQ5(region, year);

    string informTablePath = "../UserInform";
    string informLSMGroupPath = "../UserInformLSMGroup";
    string userInformPath = "../data/user_inform.csv";
    string behaviorTablePath = "../UserBehavior";
    string behaviorLSMGroupPath = "../UserBehaviorLSMGroup";
    string userBehaviorPath = "../data/user_behavior.csv";
    if (argc != 7) {
        cout << "wrong argc" << endl;
    }
    string workdir = "/home/yanhao/桌面/HITDB/storeBenchmark/workdir";
    int preLoad1 = stoi(argv[1]);
    int actualLoad1 = stoi(argv[2]);
    int preLoad2 = stoi(argv[3]);
    int actualLoad2 = stoi(argv[4]);
    int type = stoi(argv[5]);
    UserBaseResolver *user_resolver;
    switch (type) {
        case 1:
            user_resolver = new UserRowStoreResolver(informTablePath, behaviorTablePath,
                                                     userInformPath, userBehaviorPath, workdir);
            break;
        case 2:
            user_resolver = new UserColumnStoreResolver(informTablePath, behaviorTablePath,
                                                        userInformPath, userBehaviorPath, workdir);
            break;
        case 3:
            user_resolver = new UserLSMFullResolver(informTablePath, behaviorTablePath,
                                                         userInformPath, userBehaviorPath, workdir);
            break;
        case 4:
            user_resolver = new UserLSMGroupResolver(informLSMGroupPath, behaviorLSMGroupPath,
                                                     userInformPath, userBehaviorPath, workdir);
            break;
        default:
            user_resolver = new UserRowStoreResolver(informTablePath, behaviorTablePath,
                                                     userInformPath, userBehaviorPath, workdir);
            break;
    }
    user_resolver->loadData(preLoad1, actualLoad1, preLoad2, actualLoad2);
    user_resolver->createIndex();
    user_resolver->execQ1();
    user_resolver->execQ2();
    user_resolver->execQ3();
    user_resolver->execQ4();
    user_resolver->execQ5();
    delete user_resolver;
//    BaseResolver *resolver;
//    switch (type) {
//        case 1:
//            resolver = new RowStoreResolver(tablePath, lineItemPath, workdir);
//            break;
//        case 2:
//            resolver = new ColumnStoreResolver(tablePath, lineItemPath, workdir);
//            break;
//        case 3:
//            resolver = new LSMFullResolver(tablePath, lineItemPath, workdir);
//            break;
//        case 4:
//            resolver = new LSMGroupResolver(tablePath, lineItemPath, workdir);
//            break;
//        default:
//            resolver = new RowStoreResolver(tablePath, lineItemPath, workdir);
//    }
//    resolver->loadData(preLoad, actualLoad);
//    LineItemQueryResolver::waitUntilNextFlush();
//
//    std::this_thread::sleep_for(std::chrono::seconds(30));
//    resolver->readQuery1(q1);
//    LineItemQueryResolver::waitUntilNextFlush();
//
//    resolver->readQuery2(q2);
//    LineItemQueryResolver::waitUntilNextFlush();
//
//    resolver->execQ3(q3);
//    resolver->execQ5(q5);
//    delete resolver;
}

void lineItemInfo(const char *lineItemFile) {
    ifstream lineItemStream;
    lineItemStream.open(lineItemFile);
    char buffer[1024];
    int count = 0, maxLen = 0, fieldLen[16] = {};
    long averageLen = 0;
    while (lineItemStream.getline(buffer, 1024)) {
        count++;
        int actualLen = strlen(buffer);
        maxLen = actualLen > maxLen ? actualLen : maxLen;
        averageLen += actualLen;
        int pos = 0;
        for (int i = 0; i < 16; i++) {
            for (int j = pos; j < actualLen; j++) {
                if (buffer[j] == '|') {
                    int field = j - pos;
                    fieldLen[i] = field > fieldLen[i] ? field : fieldLen[i];
                    pos = j + 1;
                    break;
                }
            }
        }
    }
    averageLen = averageLen / count;
    cout << "total count:" << count << " max len:" << maxLen << " average len:" << averageLen << endl;
    for (int i = 0; i < 16; i++) {
        cout << "filed " << i + 1 << " len is: " << fieldLen[i] << endl;
    }
}