//
// Created by iron on 2019/12/3.
//

#include "WiredTigerBench.h"
#include "iostream"
#include "sstream"
#include "boost/filesystem/path.hpp"
#include "boost/filesystem/operations.hpp"
#include <cstdio>
#include <cstdlib>
#include "boost/algorithm/string/split.hpp"
#include "boost/algorithm/string.hpp"

using namespace std;
WiredTigerBench::WiredTigerBench(
        const string& folder,
        bool overwrite,
        DataSource* dataSource,
        TestStyle style) : TestBench(dataSource, style) {
    _tableName = "table:userbehavior";
    stringstream sb1, sb2;
    sb1 << SCRIPT_DIR << "/pcstat -terse " << folder << "/userbehavior.wt";
    _pcCommand = sb1.str();
    sb1.clear();

    boost::filesystem::path path(folder);
    bool needCreateTable = false;
    if (boost::filesystem::exists(path)) {
        if (overwrite) {
            system(("rm -rf " + folder).c_str());
            boost::filesystem::create_directory(path);
            needCreateTable = true;
        }
    } else {
        boost::filesystem::create_directory(path);
        needCreateTable = true;
    }
    wiredtiger_open(folder.c_str(), nullptr, "create", &_conn);
    _conn->open_session(_conn, nullptr, nullptr, &_session);

    sb2 << "key_format=" << source->getWTKeyFormat() << ",value_format=" << source->getWTValueFormat() << ",columns=("
        << source->getWTColumnNames() << ")";
    if (needCreateTable) {
        _session->create(_session, _tableName, sb2.str().c_str());
    }
}

void WiredTigerBench::close() {
    if (_conn != nullptr && _session != nullptr) {
        _session->close(_session, nullptr);
        _conn->close(_conn, nullptr);
        _session = nullptr;
        _conn = nullptr;
    }
}

void WiredTigerBench::printError(int errorNo, const char* message) {
    if (errorNo == 0) return;
    cout << message << " ";
    switch (errorNo) {
        case WT_ROLLBACK:
            cout << "ERROR:ROLLBACK" << endl;
            break;
        case WT_DUPLICATE_KEY:
            cout << "ERROR:DUPLICATE_KEY" << endl;
            break;
        case WT_ERROR:
            cout << "ERROR:ERROR" << endl;
            break;
        case WT_NOTFOUND:
            cout << "ERROR:NOTFOUND" << endl;
            break;
        case WT_PANIC:
            cout << "ERROR:PANIC" << endl;
            break;
        case WT_RUN_RECOVERY:
            cout << "ERROR:RUN_RECOVERY" << endl;
            break;
        case WT_CACHE_FULL:
            cout << "ERROR:CACHE_FULL" << endl;
            break;
        case WT_PREPARE_CONFLICT:
            cout << "ERROR:PREPARE_CONFLICT" << endl;
            break;
        case WT_TRY_SALVAGE:
            cout << "ERROR:TRY_SALVAGE" << endl;
            break;
        default:
            cout << "ERROR:UNKNOWN " << errorNo << endl;
    }
}

void WiredTigerBench::endInsert() {
    error_check(_cursor->close(_cursor), "WiredTigerBench::endInsert");
}

void WiredTigerBench::startInsert() {
    error_check(_session->open_cursor(_session, _tableName, nullptr, "append", &_cursor), "WiredTigerBench::startInsert");
}

void WiredTigerBench::error_check(int error, const char* message) {
    if (error != 0) printError(error, message);
}

int WiredTigerBench::scan(uint32_t minOrderKey, uint32_t maxOrderKey, int scanSize, int& retMinOK, int& retMaxOK) {
    uint32_t randOffset = maxOrderKey - scanSize - minOrderKey;
    randOffset = random() % randOffset;
    uint32_t startRowOrderKey = minOrderKey + randOffset;
    uint32_t endRowOrderKey = startRowOrderKey + scanSize;
    int resultSize = 0;
    int exact = 0;
    int leadKey;
    error_check(_session->open_cursor(_session, _tableName, nullptr, nullptr, &_cursor), "WiredTigerBench::scan::1");
    source->wtInitLeadKeyCursor(_cursor, startRowOrderKey);
    error_check(_cursor->search_near(_cursor, &exact), "WiredTigerBench::scan::2");
    leadKey = source->wtGetCursorLeadKey(_cursor);
    while (leadKey < startRowOrderKey) {
        int ret = _cursor->next(_cursor);
        if (ret) {
            printError(ret, "WiredTigerBench::scan::3");
            error_check(_cursor->close(_cursor), "WiredTigerBench::scan::4");
            return resultSize;
        }
        leadKey = source->wtGetCursorLeadKey(_cursor);
    }
    retMinOK = leadKey;
    retMaxOK = leadKey;
    while (leadKey < endRowOrderKey) {
        resultSize++;
        int ret = _cursor->next(_cursor);
        if (ret) {
            printError(ret, "WiredTigerBench::scan::5");
            error_check(_cursor->close(_cursor), "WiredTigerBench::scan::6");
            return resultSize;
        }
        leadKey = source->wtGetCursorLeadKey(_cursor);
    }
    retMaxOK = leadKey;
    error_check(_cursor->close(_cursor), "WiredTigerBench::scan::7");
    return resultSize;
}

PageCacheStat *WiredTigerBench::getPageCacheStat() {
    auto stat = new PageCacheStat();
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

int WiredTigerBench::insert(int rows, FixSizeSample<int>& samples) {
    int ret = 0;
    startInsert();
    for (int i = 0; i < rows; i++) {
        if (source->hasNext()) {
            int leadKey = source->wtSetNextKey(_cursor);
            source->wtSetNextValue(_cursor);
            error_check(_cursor->insert(_cursor), "WiredTigerBench::insert");
            _keyDistribution.offerSample(leadKey);
            samples.offerSample(leadKey);
            if (leadKey < _minOrderKey) _minOrderKey = leadKey;
            if (leadKey > _maxOrderKey) _maxOrderKey = leadKey;
            ret++;
        } else {
            break;
        }
    }
    endInsert();
    return ret;
}
