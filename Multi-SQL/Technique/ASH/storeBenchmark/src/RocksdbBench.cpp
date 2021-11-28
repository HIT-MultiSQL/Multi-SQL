//
// Created by iron on 2019/11/16.
//
#include "RocksdbBench.h"
#include <iostream>
#include <sstream>
#include <string>
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include <cstdio>
#include "boost/algorithm/string/split.hpp"
#include "boost/algorithm/string.hpp"
#include "boost/filesystem/path.hpp"
#include "boost/filesystem/operations.hpp"

using namespace std;

RocksdbBench::RocksdbBench(
        const std::string& path,
        bool overwrite,
        rocksdb::Options& options,
        DataSource* dataSource,
        TestStyle style): TestBench(dataSource, style), _path(path) {
    stringstream sb;
    sb << SCRIPT_DIR << "/pcstat -terse " << path << "/*.sst";
    _pcCommand = sb.str();

    boost::filesystem::path folder(path);
    if (boost::filesystem::exists(folder)) {
        if (overwrite) {
            system(("rm -rf " + path).c_str());
        }
    }

    // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
    options.IncreaseParallelism();
    options.OptimizeLevelStyleCompaction();
    // create the DB if it's not already present
    options.create_if_missing = true;
    rocksdb::Status s = rocksdb::DB::Open(options, path, &_ins);
    assert(s.ok());
}

RocksdbBench::~RocksdbBench() {
    delete _ins;
}

int RocksdbBench::scan(uint32_t minOrderKey, uint32_t maxOrderKey, int scanSize, int& retMinOK, int& retMaxOK) {
    unsigned int randOffset = maxOrderKey - scanSize - minOrderKey;
    randOffset = random() % randOffset;
    unsigned int startRowOrderKey = minOrderKey + randOffset;
    int resultSize = 0;
    string start = source->initLeadKeyString(startRowOrderKey);
    string end = source->initLeadKeyString(startRowOrderKey + scanSize);
    rocksdb::Iterator* it = _ins->NewIterator(rocksdb::ReadOptions());
    it->Seek(start);
    if (it->Valid() && it->key().ToString() < end) {
        string keyString = it->key().ToString();
        retMinOK = source->parseLeadKeyFromString(keyString);
        retMaxOK = retMinOK;
        it->Next();
        resultSize++;
        while (it->Valid() && it->key().ToString() < end) {
            keyString = it->key().ToString();
            retMaxOK = source->parseLeadKeyFromString(keyString);
            it->Next();
            resultSize++;
        }
    }
    assert(it->status().ok()); // Check for any errors found during the scan
    delete it;
    return resultSize;
}

rocksdb::DB *RocksdbBench::getInstance() {
    return _ins;
}

PageCacheStat *RocksdbBench::getPageCacheStat() {
    auto stat = new PageCacheStat();
    if (noFileExist) {
        return stat;
    }
    // execute pcstat command
    FILE* pipe = popen(_pcCommand.c_str(), "r");
    if (!pipe) {
        cout << "ERROR in RocksdbBench::getPageCacheStat" << endl;
        return nullptr;
    }
    char buffer[256];
    string resultString;
    while(!feof(pipe)) {
        if(fgets(buffer, 128, pipe) != nullptr)
            resultString += buffer;
    }
    pclose(pipe);

    vector<string> lines;
    boost::split(lines, resultString, boost::is_any_of( "\n" ) );
    // first line is header; last line is blank
    for (int i = 1; i < lines.size() - 1; i++) {
        vector<string> fields;
        boost::split(fields, lines[i], boost::is_any_of( "," ));
        stat->insert(fields[0], stol(fields[1]), stol(fields[4]), stol(fields[5]));
        stat->setTimeStamp(stol(fields[2]));
    }
    return stat;
}

void RocksdbBench::getFileMeta(int &outL1, int &outL2, int &outMaxLevel, long &totalSize, long &totalEntries) {
    auto result = new vector<rocksdb::LiveFileMetaData>;
    _ins->GetLiveFilesMetaData(result);
    outL1 = 0;
    outL2 = 0;
    outMaxLevel = 0;
    totalSize = 0;
    totalEntries = 0;
    noFileExist = true;
    for (auto & i : *result) {
        noFileExist = false;
        int level = i.level;
        outMaxLevel = level > outMaxLevel ? level : outMaxLevel;
        if (level == 1) {
            outL1++;
        } else {
            outL2++;
        }
        totalSize += i.size;
        totalEntries += i.num_entries;
    }
    delete result;
}

int RocksdbBench::insert(int rows, FixSizeSample<int>& samples) {
    int ret = 0;
    for (int i = 0; i < rows; i++) {
        if (source->hasNext()) {
            string keyString = source->getNextKeyString();
            _ins->Put(_writeOptions, keyString, source->getNextValueString());
            int leadKey = source->getCurrentLeadKey();
            _keyDistribution.offerSample(leadKey);
            samples.offerSample(leadKey);
            if (leadKey < _minOrderKey) _minOrderKey = leadKey;
            if (leadKey > _maxOrderKey) _maxOrderKey = leadKey;
            ret++;
        } else {
            break;
        }
    }
    return ret;
}
