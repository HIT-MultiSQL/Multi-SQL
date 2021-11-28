//
// Created by ironwei on 2020/3/18.
//

#include "WiredTigerColumnBench.h"
#include "WiredTigerBench.h"
#include "iostream"
#include "sstream"
#include "boost/filesystem/path.hpp"
#include "boost/filesystem/operations.hpp"
#include <cstdio>
#include <cstdlib>
#include "boost/algorithm/string.hpp"

//#define DEBUG_WY

WiredTigerColumnBench::WiredTigerColumnBench(const string &folder, bool overwrite, ColumnStoreSource *dataSource,
                                             TestStyle style): TestBench(dataSource, style) {
    _tableName = "table:userinformcol";
    _colgroupPrefix = "colgroup:userinformcol:";
    stringstream sb1;
    sb1 << SCRIPT_DIR << "/pcstat -terse " << folder << "/*.wt";
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


    vector<string> fields = dynamic_cast<ColumnStoreSource*>(source)->getFields();
    string colgroups = boost::algorithm::join(fields, ",");
    if (needCreateTable) {
        string leadKey =  dynamic_cast<ColumnStoreSource*>(source)->getLeadKeyField();
        boost::algorithm::to_lower(colgroups);
        colgroups = "main," + colgroups;
        string config = "key_format=" + source->getWTKeyFormat() + ",value_format=" + source->getWTValueFormat() + ",columns=("
                        + source->getWTColumnNames() + "),colgroups=(" + colgroups + ")";
#ifdef DEBUG_WY
        cout << config << endl;
#endif
        string leadKeyColumn = "columns=(" + leadKey + ")";
        WiredTigerBench::error_check(_session->create(_session, _tableName, config.c_str()),
                                     "WiredTigerColumnBench::WiredTigerColumnBench() create table");
        string main = string(_colgroupPrefix) + "main";
        WiredTigerBench::error_check(_session->create(_session, main.c_str(), leadKeyColumn.c_str()),
                                     "WiredTigerColumnBench::WiredTigerColumnBench() create main colgroup");
        for (int i = 0; i < fields.size(); i++) {
            string colgroup = string(_colgroupPrefix) + fields[i];
            boost::algorithm::to_lower(colgroup);
            string columns = "columns=(" + fields[i] + ")";
            WiredTigerBench::error_check(_session->create(_session, colgroup.c_str(), columns.c_str()),
                                         "WiredTigerColumnBench::WiredTigerColumnBench() create main colgroup");
        }
    }


}

void WiredTigerColumnBench::endInsert() {
    WiredTigerBench::error_check(_cursor->close(_cursor), "WiredTigerColumnBench::endInsert");
}

void WiredTigerColumnBench::startInsert() {
    WiredTigerBench::error_check(_session->open_cursor(_session, _tableName, nullptr, "append", &_cursor), "WiredTigerColumnBench::startInsert");
}
void WiredTigerColumnBench::close() {
    if (_conn != nullptr && _session != nullptr) {
        _session->close(_session, nullptr);
        _conn->close(_conn, nullptr);
        _session = nullptr;
        _conn = nullptr;
    }
}

PageCacheStat *WiredTigerColumnBench::getPageCacheStat() {
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

int WiredTigerColumnBench::insert(int rows, FixSizeSample<int>& samples) {
    int ret = 0;
    startInsert();
    for (int i = 0; i < rows; i++) {
        if (source->hasNext()) {
            int leadKey = source->wtSetNextKey(_cursor);
            source->wtSetNextValue(_cursor);
            WiredTigerBench::error_check(_cursor->insert(_cursor), "WiredTigerColumnBench::insert");
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

int WiredTigerColumnBench::scan(uint32_t minOrderKey, uint32_t maxOrderKey, int scanSize, int& retMinOK, int& retMaxOK) {
    return 0;
}

long WiredTigerColumnBench::fullScan(WT_SESSION* session, string &_targetColumnName, bool isInt, int& fieldSize) {
    WT_CURSOR* _cursor = nullptr;
    long resultSize = 0;
    WiredTigerBench::error_check(session->open_cursor(session, _targetColumnName.c_str(), nullptr, nullptr, &_cursor), "WiredTigerColumnBench::fullScan::1");
    int ret;
    const char* strVal;
    uint32_t intVal;
    while ((ret = _cursor->next(_cursor)) == 0) {
        if (isInt) {
            WiredTigerBench::error_check(_cursor->get_value(_cursor, &intVal), "WiredTigerColumnBench::fullScan::2");
        } else {
            WiredTigerBench::error_check(_cursor->get_value(_cursor, &strVal), "WiredTigerColumnBench::fullScan::3");
        }
        resultSize++;
    }
    if (ret != WT_NOTFOUND) {
        WiredTigerBench::error_check(ret, "WiredTigerColumnBench::fullScan::4");
    }
    WiredTigerBench::error_check(_cursor->close(_cursor), "WiredTigerColumnBench::scan::7");
    if (isInt) {
        fieldSize = 4;
    } else {
        fieldSize = strlen(strVal);
    }
    return resultSize;
}
