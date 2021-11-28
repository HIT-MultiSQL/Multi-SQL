//
// Created by ironwei on 2020/5/9.
//

#include "Q3.h"
#include <bitset>
#include "boost/algorithm/string/split.hpp"
#include "../WiredTigerBench.h"

void Q3::prepareQ3(string segment, int dayOfMonth, vector<int>& orderKeys, vector<string>& orderDates, vector<int>& shipPriors, vector<int>& sortPos) {
    WT_CURSOR* _cursor = nullptr;
    auto startTime = chrono::system_clock::now();
    vector<uint32_t> custKeys;
    WiredTigerBench::error_check(_COSession->open_cursor(_COSession, "index:customer:segment(CUSTKEY)", nullptr, nullptr, &_cursor),
                                 "Q3::prepareQ3():0");
    char segmentArr[10];
    memset(segmentArr, 0, 10);
    memcpy(segmentArr, segment.c_str(), segment.size());
    char* retSegment;
    uint32_t custKey;
    int ret;
    _cursor->set_key(_cursor, segmentArr);
    WiredTigerBench::error_check(_cursor->search(_cursor), "Q3::prepareQ3():1");
    _cursor->get_key(_cursor, &retSegment);
    ret = _cursor->get_value(_cursor, &custKey);
    WiredTigerBench::error_check(ret, "Q3::prepareQ3():2");
    if (strncmp(retSegment, segmentArr, 10) == 0) {
        custKeys.push_back(custKey);
#ifdef DEBUG_WY
        cout << "custKey:" << custKey << " seg:" << retSegment << endl;
#endif
        while ((ret = _cursor->next(_cursor)) == 0) {
            _cursor->get_key(_cursor, &retSegment);
            WiredTigerBench::error_check(_cursor->get_value(_cursor, &custKey), "Q3::prepareQ3():3");
            if (strncmp(retSegment, segmentArr, 10) == 0) {
                custKeys.push_back(custKey);
#ifdef DEBUG_WY
                cout << "custKey:" << custKey << " seg:" << retSegment << endl;
#endif
            } else {
                break;
            }
        }
    }
    if (ret != 0 && ret != WT_NOTFOUND) {
        WiredTigerBench::error_check(ret, "Q3::prepareQ3():4");
    }
    WiredTigerBench::error_check(_cursor->close(_cursor), "Q3::prepareQ3():5");
    auto endTime = chrono::system_clock::now();
    auto duration = chrono::duration_cast<chrono::milliseconds>(endTime - startTime);
    cout << "customer use Time:" << duration.count() << "ms" << endl;

    startTime = chrono::system_clock::now();
    WiredTigerBench::error_check(_COSession->open_cursor(_COSession, "index:orders:custkey(ORDERKEY,ORDERDATE,SHIPPRIORITY)", nullptr, nullptr, &_cursor),
                                 "Q3::prepareQ3():6");
    char* orderDate;
    int retCustKey;
    int shipPrior;
    int orderKey;
    int retDayOfMonth;
    int retMonth;
    int retYear;

    for (auto key : custKeys) {
        _cursor->set_key(_cursor, key);
        ret = _cursor->search(_cursor);
        if (ret != 0 && ret != WT_NOTFOUND) {
            WiredTigerBench::error_check(ret, "Q3::prepareQ3():7");
        }
        if (ret == 0) {
            _cursor->get_key(_cursor, &retCustKey);
            if (retCustKey == key) {
                _cursor->get_value(_cursor, &orderKey, &orderDate, &shipPrior);
                retYear = (orderDate[0] - '0') * 1000 + (orderDate[1] - '0') * 100 + (orderDate[2] - '0') * 10 + (orderDate[3] - '0');
                retMonth = (orderDate[5] - '0') * 10 + (orderDate[6] - '0');
                retDayOfMonth = (orderDate[8] - '0') * 10 + (orderDate[9] - '0');
                if (retYear < 1995 || (retYear == 1995 && retMonth < 3) || (retYear == 1995 && retMonth == 3 && retDayOfMonth < dayOfMonth)) {
                    orderKeys.push_back(orderKey);
                    orderDates.emplace_back(string(orderDate,0,10));
                    shipPriors.emplace_back(shipPrior);
                }
            }
            while ((ret = _cursor->next(_cursor)) == 0) {
                _cursor->get_key(_cursor, &retCustKey);
                if (retCustKey == key) {
                    _cursor->get_value(_cursor, &orderKey, &orderDate, &shipPrior);
                    retYear = (orderDate[0] - '0') * 1000 + (orderDate[1] - '0') * 100 + (orderDate[2] - '0') * 10 + (orderDate[3] - '0');
                    retMonth = (orderDate[5] - '0') * 10 + (orderDate[6] - '0');
                    retDayOfMonth = (orderDate[8] - '0') * 10 + (orderDate[9] - '0');
                    if (retYear < 1995 || (retYear == 1995 && retMonth < 3) || (retYear == 1995 && retMonth == 3 && retDayOfMonth < dayOfMonth)) {
                        orderKeys.push_back(orderKey);
                        orderDates.emplace_back(string(orderDate,0,10));
                        shipPriors.emplace_back(shipPrior);
                    }
                } else {
                    break;
                }
            }
        }

        if (ret != 0 && ret != WT_NOTFOUND) {
            WiredTigerBench::error_check(ret, "Q3::prepareQ3():8");
        }
    }
    WiredTigerBench::error_check(_cursor->close(_cursor), "Q3::prepareQ3():9");

    endTime = chrono::system_clock::now();
    duration = chrono::duration_cast<chrono::milliseconds>(endTime - startTime);
    cout << "orders use Time:" << duration.count() << "ms, found " << orderKeys.size() << endl;

    startTime = chrono::system_clock::now();
    sortPos.reserve(orderKeys.size());
    for (int i = 0; i < orderKeys.size(); i++) {
        sortPos.push_back(i);
    }
    sort(sortPos.begin(), sortPos.end(), [&orderKeys](int l, int r) { return orderKeys[l] < orderKeys[r]; });
    duration = chrono::duration_cast<chrono::milliseconds>(endTime - startTime);
    cout << "sort use Time:" << duration.count() << "ms" << endl;
}

void Q3::batchExecQ3(vector<string> &segments, vector<int> &dayOfMonths) {
    vector<int> orderKeys;
    vector<string> orderDates;
    vector<int> shipPrior;
    vector<int> sortPos;

    vector<Q3Result> results;
    for (int i = 0; i < segments.size(); i++) {
        orderKeys.clear();
        orderDates.clear();
        shipPrior.clear();
        sortPos.clear();
        prepareQ3(segments[i], dayOfMonths[i], orderKeys, orderDates, shipPrior, sortPos);

        // lineItem table
        if (EXEC_BT) {
            WT_CURSOR* _cursor = nullptr;
            auto startTime = chrono::system_clock::now();
            WiredTigerBench::error_check(_LISession->open_cursor(_LISession,
                                                                 "table:lineitem(ORDERKEY,PARTKEY,SUPPKEY,LINENUMBER,EXTENDPRICE,DISCOUNT,SHIPDATE)",
                                                                 nullptr, nullptr, &_cursor),
                                         "Q3::batchExecQ3 open lineitem");
            results.clear();
            for (int j = 0; j < orderKeys.size(); j++) {
                int orderKey = orderKeys[sortPos[j]];

                _cursor->set_key(_cursor, orderKey, 0, 0, 0);
                int exact;
                int ret = _cursor->search_near(_cursor, &exact);
                int retOrderKey;
                int retPartKey;
                int retSuppKey;
                const char *extendPrice;
                const char *discount;
                const char *shipdate;
                u_int16_t retLineNumber;
                double sumRevenue = 0;

                while (ret == 0 && exact < 0) {
                    WiredTigerBench::error_check(
                            _cursor->get_key(_cursor, &retOrderKey, &retPartKey, &retSuppKey, &retLineNumber),
                            "Q3::batchExecQ3 lineitem get key");
                    if (retOrderKey >= orderKey) {
                        break;
                    }
                    ret = _cursor->next(_cursor);
                }

                if (ret != WT_NOTFOUND) {
                    WiredTigerBench::error_check(ret, "Q3::batchExecQ3 lineitem search");
                    if (ret == 0) {
                        do {
                            WiredTigerBench::error_check(
                                    _cursor->get_value(_cursor, &retOrderKey, &retPartKey, &retSuppKey, &retLineNumber,
                                                       &extendPrice, &discount, &shipdate),
                                    "Q3::batchExecQ3 lineitem get value");
                            if (retOrderKey != orderKey) {
                                break;
                            }

                            int retYear;
                            int retMonth;
                            int retDayOfMonth;
                            retYear =
                                    (shipdate[0] - '0') * 1000 + (shipdate[1] - '0') * 100 + (shipdate[2] - '0') * 10 +
                                    (shipdate[3] - '0');
                            retMonth = (shipdate[5] - '0') * 10 + (shipdate[6] - '0');
                            retDayOfMonth = (shipdate[8] - '0') * 10 + (shipdate[9] - '0');
                            if (retYear > 1995 || (retYear == 1995 && retMonth > 3) ||
                                (retYear == 1995 && retMonth == 3 && retDayOfMonth > dayOfMonths[i])) {
                                sumRevenue += stod(extendPrice) * (1 - stod(discount));
                            }
                        } while (_cursor->next(_cursor) == 0);
                    }
                }

                if (ret != WT_NOTFOUND) {
                    WiredTigerBench::error_check(ret, "Q3::batchExecQ3 lineitem next");
                }
                if (sumRevenue > 0) {
#ifdef DEBUG_WY
                    string orderDate = string(orderDates[j], 0, 10);
                    cout << "OK:" << orderKey << " RE:" << sumRevenue << " OD:" << orderDate << " SP:" << shipPrior[j]
                         << endl;
#endif
                    results.emplace_back(Q3Result(orderKey, sumRevenue, orderDates[sortPos[j]], shipPrior[sortPos[j]]));
                    push_heap(results.begin(), results.end());
                    if (results.size() > 10) {
                        pop_heap(results.begin(), results.end());
                        results.pop_back();
                    }
                }
            }
            WiredTigerBench::error_check(_cursor->close(_cursor), "Q3::prepareQ3():9");
            auto endTime = chrono::system_clock::now();
            auto duration = chrono::duration_cast<chrono::milliseconds>(endTime - startTime);
            cout << "Q3 B+ use Time:" << duration.count() << "ms" << endl;

            sort_heap(results.begin(), results.end());
            for (int j = 0; j < results.size(); j++) {
                cout << "OK:" << results[j].orderKey << " RE:" << results[j].revenue << " OD:" << results[j].orderDate
                     << " SP:" << results[j].shipPriority << endl;
            }
        }

        // lineItemCOlTable scan style
        if (EXEC_COL_SCAN) {
            WT_CURSOR* _cursor = nullptr;
            auto startTime = chrono::system_clock::now();
            map<int, double> revenue;
            bitset<12000008> targer_ok;
            for (auto& val: orderKeys) {
                targer_ok[val] = true;
            }
            bitset<12000008> ok_set;
            // scan orderKey
            WiredTigerBench::error_check(_LISession->open_cursor(_LISession, "colgroup:lineitemcol:orderkey",
                                                                 nullptr, nullptr, &_cursor),"");
            int ret;
            uint32_t intVal;
            unsigned int currentID = 0;
            while ((ret = _cursor->next(_cursor)) == 0) {
                WiredTigerBench::error_check(_cursor->get_value(_cursor, &intVal), "");
                if (targer_ok[intVal]) {
                    ok_set[currentID] = true;
                }
                currentID++;
            }
            if (ret != WT_NOTFOUND) {
                WiredTigerBench::error_check(ret, "");
            }
            WiredTigerBench::error_check(_cursor->close(_cursor), "");
            // scan shipDate
            WiredTigerBench::error_check(_LISession->open_cursor(_LISession, "colgroup:lineitemcol:shipdate",
                                                                 nullptr, nullptr, &_cursor),"");
            const char* strVal;
            currentID = 0;
            while ((ret = _cursor->next(_cursor)) == 0) {
                if (ok_set[currentID]) {
                    WiredTigerBench::error_check(_cursor->get_value(_cursor, &strVal), "");
                    int retYear;
                    int retMonth;
                    int retDayOfMonth;
                    retYear =
                            (strVal[0] - '0') * 1000 + (strVal[1] - '0') * 100 + (strVal[2] - '0') * 10 +
                            (strVal[3] - '0');
                    retMonth = (strVal[5] - '0') * 10 + (strVal[6] - '0');
                    retDayOfMonth = (strVal[8] - '0') * 10 + (strVal[9] - '0');
                    if (retYear < 1995 || (retYear == 1995 && retMonth < 3) ||
                        (retYear == 1995 && retMonth == 3 && retDayOfMonth <= dayOfMonths[i])) {
                        ok_set[currentID] = false;
                    }
                }
                currentID++;
            }
            if (ret != WT_NOTFOUND) {
                WiredTigerBench::error_check(ret, "");
            }
            WiredTigerBench::error_check(_cursor->close(_cursor), "");

            WT_CURSOR* _cursor2;
            WT_CURSOR* _cursor3;
            WiredTigerBench::error_check(_LISession->open_cursor(_LISession, "colgroup:lineitemcol:extendprice",
                                                                 nullptr, nullptr, &_cursor),"");
            WiredTigerBench::error_check(_LISession->open_cursor(_LISession, "colgroup:lineitemcol:discount",
                                                                 nullptr, nullptr, &_cursor2), "");
            WiredTigerBench::error_check(_LISession->open_cursor(_LISession, "colgroup:lineitemcol:orderkey",
                                                                 nullptr, nullptr, &_cursor3), "");
            currentID = 0;
            const char* strVal2;
            while (((ret = _cursor->next(_cursor)) == 0) && ((ret = _cursor2->next(_cursor2)) == 0) && ((ret = _cursor3->next(_cursor3)) == 0)) {
                if (ok_set[currentID]) {
                    WiredTigerBench::error_check(_cursor->get_value(_cursor, &strVal), "");
                    WiredTigerBench::error_check(_cursor2->get_value(_cursor2, &strVal2), "");
                    WiredTigerBench::error_check(_cursor3->get_value(_cursor3, &intVal), "");
                    double extendedPrice = stod(strVal);
                    double discount = stod(strVal2);
                    if (revenue.count(intVal) == 0) {
                        revenue[intVal] = extendedPrice * (1 - discount);
                    } else {
                        revenue[intVal] += extendedPrice * (1 - discount);
                    }
                }
                currentID++ ;
            }
            if (ret != WT_NOTFOUND) {
                WiredTigerBench::error_check(ret, "");
            }
            WiredTigerBench::error_check(_cursor->close(_cursor), "");
            WiredTigerBench::error_check(_cursor2->close(_cursor2), "");
            WiredTigerBench::error_check(_cursor3->close(_cursor3), "");

            for (int j = 0; j < orderKeys.size(); j++) {
                int pos = sortPos[j];
                int orderKey = orderKeys[pos];
                if (orderKey == 519556) {
                    cout << "";
                }
                if (revenue.count(orderKey)) {
                    results.emplace_back(Q3Result(orderKey, revenue[orderKey], orderDates[pos], shipPrior[pos]));
                    push_heap(results.begin(), results.end());
                    if (results.size() > 10) {
                        pop_heap(results.begin(), results.end());
                        results.pop_back();
                    }
                }
            }
            auto endTime = chrono::system_clock::now();
            auto duration = chrono::duration_cast<chrono::milliseconds>(endTime - startTime);
            cout << "Q5 Col use time:" << duration.count() << "ms" << endl;

            sort_heap(results.begin(), results.end());
            for (int j = 0; j < results.size(); j++) {
                cout << "OK:" << results[j].orderKey << " RE:" << results[j].revenue << " OD:" << results[j].orderDate
                     << " SP:" << results[j].shipPriority << endl;
            }
        }

        // lineItem LSM
        if (EXEC_LSM) {
            auto startTime = chrono::system_clock::now();
            char startKeyBuf[LINEITEM_KEY_LEN];
            char endKeyBuf[LINEITEM_KEY_LEN];
            results.clear();

            for (int j = 0; j < orderKeys.size(); j++) {
                int orderKey = orderKeys[sortPos[j]];
                sprintf(startKeyBuf, _keyFormat.c_str(), orderKey, 0, 0, 0);
                sprintf(endKeyBuf, _keyFormat.c_str(), orderKey + 1, 0, 0, 0);
                string start(startKeyBuf);
                string end(endKeyBuf);
                rocksdb::Iterator* it = _ins->NewIterator(rocksdb::ReadOptions());
                it->Seek(start);
                int splitNum;
                string extendPrice;
                int extendStart;
                int extendEnd;
                string discount;
                int discountStart;
                int discountEnd;
                int shipDateStart;
                int retYear;
                int retMonth;
                int retDayOfMonth;
                double sumRevenue = 0;
                while (it->Valid() && it->key().ToString() < end) {
                    string keyString = it->key().ToString();
                    string valueString = it->value().ToString();
                    splitNum = 0;
                    for (int k = 0; k < valueString.size(); k++) {
                        char ch = valueString[k];
                        if (ch == '|') {
                            splitNum++;
                            if (splitNum == 1) {
                                extendStart = k + 1;
                            } else if (splitNum == 2) {
                                extendEnd = k;
                                discountStart = k + 1;
                            } else if (splitNum == 3) {
                                discountEnd = k;
                            } else if (splitNum == 6) {
                                shipDateStart = k + 1;
                                break;
                            }
                        }
                    }

                    retYear = (valueString[shipDateStart] - '0') * 1000 + (valueString[shipDateStart + 1] - '0') * 100 +
                              (valueString[shipDateStart + 2] - '0') * 10 + (valueString[shipDateStart + 3] - '0');
                    retMonth = (valueString[shipDateStart + 5] - '0') * 10 + (valueString[shipDateStart + 6] - '0');
                    retDayOfMonth = (valueString[shipDateStart + 8] - '0') * 10 + (valueString[shipDateStart + 9] - '0');
                    if (retYear > 1995 || (retYear == 1995 && retMonth > 3) ||
                        (retYear == 1995 && retMonth == 3 && retDayOfMonth > dayOfMonths[i])) {
                        extendPrice = valueString.substr(extendStart, extendEnd - extendStart);
                        discount = valueString.substr(discountStart, discountEnd - discountStart);
                        sumRevenue += stod(extendPrice) * (1 - stod(discount));
                    }
                    it->Next();
                }
                assert(it->status().ok()); // Check for any errors found during the scan
                delete it;

                if (sumRevenue > 0) {
#ifdef DEBUG_WY
                    string orderDate = string(orderDates[j], 0, 10);
                    cout << "OK:" << orderKey << " RE:" << sumRevenue << " OD:" << orderDates[j] << " SP:" << shipPrior[j]
                         << endl;
#endif
                    results.emplace_back(Q3Result(orderKey, sumRevenue, orderDates[sortPos[j]], shipPrior[sortPos[j]]));
                    push_heap(results.begin(), results.end());
                    if (results.size() > 10) {
                        pop_heap(results.begin(), results.end());
                        results.pop_back();
                    }
                }
            }
            auto endTime = chrono::system_clock::now();
            auto duration = chrono::duration_cast<chrono::milliseconds>(endTime - startTime);
            cout << "Q3 LSM use Time:" << duration.count() << "ms" << endl;

            sort_heap(results.begin(), results.end());
            for (int j = 0; j < results.size(); j++) {
                cout << "OK:" << results[j].orderKey << " RE:" << results[j].revenue << " OD:" << results[j].orderDate
                     << " SP:" << results[j].shipPriority << endl;
            }
        }

        if (EXEC_LSM_GROUP) {
            auto startTime = chrono::system_clock::now();
            char startKeyBuf[LINEITEM_KEY_LEN];
            char endKeyBuf[LINEITEM_KEY_LEN];
            results.clear();

            for (int j = 0; j < orderKeys.size(); j++) {
                int orderKey = orderKeys[sortPos[j]];
                sprintf(startKeyBuf, _keyFormat.c_str(), orderKey, 0, 0, 0);
                sprintf(endKeyBuf, _keyFormat.c_str(), orderKey + 1, 0, 0, 0);
                string start(startKeyBuf);
                string end(endKeyBuf);
                rocksdb::Iterator* it = _ins->NewIterator(rocksdb::ReadOptions(), _mainGroup);
                it->Seek(start);
                string extendPrice;
                int extendStart = 0;
                int extendEnd;
                string discount;
                int discountStart;
                int discountEnd;
                int shipDateStart;
                int retYear;
                int retMonth;
                int retDayOfMonth;
                double sumRevenue = 0;
                while (it->Valid() && it->key().ToString() < end) {
                    string keyString = it->key().ToString();
                    string valueString = it->value().ToString();
                    int lpos = valueString.find('|');
                    int rpos = valueString.find('|', lpos + 1);
                    extendEnd = lpos;
                    discountStart = lpos + 1;
                    discountEnd = rpos;
                    shipDateStart = rpos + 1;

                    retYear = (valueString[shipDateStart] - '0') * 1000 + (valueString[shipDateStart + 1] - '0') * 100 +
                              (valueString[shipDateStart + 2] - '0') * 10 + (valueString[shipDateStart + 3] - '0');
                    retMonth = (valueString[shipDateStart + 5] - '0') * 10 + (valueString[shipDateStart + 6] - '0');
                    retDayOfMonth = (valueString[shipDateStart + 8] - '0') * 10 + (valueString[shipDateStart + 9] - '0');
                    if (retYear > 1995 || (retYear == 1995 && retMonth > 3) ||
                        (retYear == 1995 && retMonth == 3 && retDayOfMonth > dayOfMonths[i])) {
                        extendPrice = valueString.substr(extendStart, extendEnd - extendStart);
                        discount = valueString.substr(discountStart, discountEnd - discountStart);
                        sumRevenue += stod(extendPrice) * (1 - stod(discount));
                    }
                    it->Next();
                }
                assert(it->status().ok()); // Check for any errors found during the scan
                delete it;

                if (sumRevenue > 0) {
#ifdef DEBUG_WY
                    string orderDate = string(orderDates[j], 0, 10);
                    cout << "OK:" << orderKey << " RE:" << sumRevenue << " OD:" << orderDates[j] << " SP:" << shipPrior[j]
                         << endl;
#endif
                    results.emplace_back(Q3Result(orderKey, sumRevenue, orderDates[sortPos[j]], shipPrior[sortPos[j]]));
                    push_heap(results.begin(), results.end());
                    if (results.size() > 10) {
                        pop_heap(results.begin(), results.end());
                        results.pop_back();
                    }
                }
            }
            auto endTime = chrono::system_clock::now();
            auto duration = chrono::duration_cast<chrono::milliseconds>(endTime - startTime);
            cout << "Q3 LSM use Time:" << duration.count() << "ms" << endl;

            sort_heap(results.begin(), results.end());
            for (int j = 0; j < results.size(); j++) {
                cout << "OK:" << results[j].orderKey << " RE:" << results[j].revenue << " OD:" << results[j].orderDate
                     << " SP:" << results[j].shipPriority << endl;
            }
        }
    }

}

void Q3::setColumnFamilyHandle(rocksdb::ColumnFamilyHandle* mainGroup, rocksdb::ColumnFamilyHandle* otherGroup) {
    _mainGroup = mainGroup;
    _otherGroup = otherGroup;
}