//
// Created by iron on 2019/12/9.
//

#ifndef STOREBENCHMARK_PAGECACHESTAT_H
#define STOREBENCHMARK_PAGECACHESTAT_H

#include <vector>
#include <string>

using namespace std;
class PageCacheStat {
public:
    void insert(string &fileName, long fileSize, long page, long cached);
    void setTimeStamp(long timeStamp);
    long totalSize = 0;
    long totalPages = 0;
    long totalCached = 0;
    long timeStamp;
    vector<string> fileNames;
    vector<long> fileSizes;
    vector<long> filePages;
    vector<long> fileCached;
};


#endif //STOREBENCHMARK_PAGECACHESTAT_H
