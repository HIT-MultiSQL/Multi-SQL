//
// Created by iron on 2019/12/9.
//

#include "PageCacheStat.h"

void PageCacheStat::setTimeStamp(long timeStamp) {
    this->timeStamp = timeStamp;
}

void PageCacheStat::insert(string &fileName, long fileSize, long page, long cached) {
    fileNames.push_back(fileName);
    fileSizes.push_back(fileSize);
    filePages.push_back(page);
    fileCached.push_back(cached);
    totalCached += cached;
    totalPages += page;
    totalSize += fileSize;
}
