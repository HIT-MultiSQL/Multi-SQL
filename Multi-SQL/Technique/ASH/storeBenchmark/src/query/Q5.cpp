//
// Created by ironwei on 2020/5/9.
//

#include "Q5.h"
#include <bitset>
#include <set>
#include <iomanip>
#include "../WiredTigerBench.h"
void Q5::batchExecQ5(vector<string> &region, vector<int> &year) {
    vector<pair<int, string>> countryName;
    vector<pair<int, int>> suppNation;
    vector<pair<int, int>> orderNation;
    for (int i = 0; i < region.size(); i++){
        prepareQ5(region[i], year[i], countryName, suppNation, orderNation);
        if (EXEC_BT) {
            auto startTime = chrono::system_clock::now();
            map<int, set<int>> nationSupp;
            for (auto pair: suppNation) {
                int nation = pair.second;
                int supp = pair.first;
                if (nationSupp.count(nation) == 0) {
                    set<int> tmp;
                    nationSupp[nation] = tmp;
                }
                nationSupp[nation].insert(supp);
            }
            WT_CURSOR* cursor;
            WiredTigerBench::error_check(_LISession->open_cursor(_LISession, "table:lineitem(EXTENDPRICE,DISCOUNT)",
                                                                 nullptr, nullptr, &cursor), "");
            map<int, double> revenue;
            int ret;
            int retOrderKey;
            int retPartKey;
            int retLineNumber;
            int retSuppKey;
            const char* retPrice;
            const char* retDiscount;
            for (auto orderNationPair: orderNation) {
                int orderKey = orderNationPair.first;
                int nationKey = orderNationPair.second;
                cursor->set_key(cursor, orderKey, 0, 0, 0);
                int exact;
                ret = cursor->search_near(cursor, &exact);
                while (ret == 0 && exact < 0) {
                    WiredTigerBench::error_check(
                            cursor->get_key(cursor, &retOrderKey, &retPartKey, &retSuppKey, &retLineNumber),
                            "");
                    if (retOrderKey >= orderKey) {
                        break;
                    }
                    ret = cursor->next(cursor);
                }

                while (ret == 0) {
                    cursor->get_key(cursor, &retOrderKey, &retPartKey, &retSuppKey, &retLineNumber);
                    if (retOrderKey != orderKey) {
                        break;
                    }
                    if (nationSupp[nationKey].count(retSuppKey) > 0) {
                        cursor->get_value(cursor, &retPrice, &retDiscount);
                        double price = stod(retPrice) * (1 - stod(retDiscount));
                        if (revenue.count(nationKey) > 0) {
                            revenue[nationKey] += price;
                        } else {
                            revenue[nationKey] = price;
                        }
                    }
                    ret = cursor->next(cursor);
                }

                if (ret != 0 && ret != WT_NOTFOUND) {
                    WiredTigerBench::error_check(ret, "");
                }
            }
            WiredTigerBench::error_check(cursor->close(cursor), "");
            auto endTime = chrono::system_clock::now();
            auto duration = chrono::duration_cast<chrono::milliseconds>(endTime - startTime);
            cout << "Q5 B+ use time:" << duration.count() << "ms" << endl;
            printResult(revenue, countryName);

        }
        if (EXEC_COL_SCAN) {
            auto startTime = chrono::system_clock::now();
            map<int, set<int>> nationSupp;
            unordered_map<int, int> orderNationMap;
            for (auto pair: suppNation) {
                int nation = pair.second;
                int supp = pair.first;
                if (nationSupp.count(nation) == 0) {
                    set<int> tmp;
                    nationSupp[nation] = tmp;
                }
                nationSupp[nation].insert(supp);
            }
            for (auto pair: orderNation) {
                orderNationMap[pair.first] = pair.second;
            }
            WT_CURSOR* _cursor1 = nullptr;
            WT_CURSOR* _cursor2 = nullptr;
            WT_CURSOR* _cursor3 = nullptr;
            WT_CURSOR* _cursor4 = nullptr;
            WiredTigerBench::error_check(_LISession->open_cursor(_LISession, "colgroup:lineitemcol:orderkey",
                                                                 nullptr, nullptr, &_cursor1), "");
            WiredTigerBench::error_check(_LISession->open_cursor(_LISession, "colgroup:lineitemcol:suppkey",
                                                                 nullptr, nullptr, &_cursor2), "");
            WiredTigerBench::error_check(_LISession->open_cursor(_LISession, "colgroup:lineitemcol:extendprice",
                                                                 nullptr, nullptr, &_cursor3), "");
            WiredTigerBench::error_check(_LISession->open_cursor(_LISession, "colgroup:lineitemcol:discount",
                                                                 nullptr, nullptr, &_cursor4), "");
            map<int, double> revenue;
            int ret;
            int retOrderKey;
            int retSuppKey;
            const char* retPrice;
            const char* retDiscount;
            while ((ret = _cursor1->next(_cursor1)) == 0) {
                _cursor2->next(_cursor2);
                _cursor3->next(_cursor3);
                _cursor4->next(_cursor4);
                _cursor1->get_value(_cursor1, &retOrderKey);
                if (orderNationMap.count(retOrderKey) > 0) {
                    _cursor2->get_value(_cursor2, &retSuppKey);
                    int nationKey = orderNationMap[retOrderKey];
                    if (nationSupp[nationKey].count(retSuppKey) > 0) {
                        _cursor3->get_value(_cursor3, &retPrice);
                        _cursor4->get_value(_cursor4, &retDiscount);
                        double price = stod(retPrice) * (1 - stod(retDiscount));
                        if (revenue.count(nationKey) > 0) {
                            revenue[nationKey] += price;
                        } else {
                            revenue[nationKey] = price;
                        }
                    }
                }
            }
            if (ret != WT_NOTFOUND) {
                WiredTigerBench::error_check(ret, "");
            }
            WiredTigerBench::error_check(_cursor1->close(_cursor1), "");
            WiredTigerBench::error_check(_cursor2->close(_cursor2), "");
            WiredTigerBench::error_check(_cursor3->close(_cursor3), "");
            WiredTigerBench::error_check(_cursor4->close(_cursor4), "");
            auto endTime = chrono::system_clock::now();
            auto duration = chrono::duration_cast<chrono::milliseconds>(endTime - startTime);
            cout << "Q5 Col use time:" << duration.count() << "ms" << endl;
            printResult(revenue, countryName);
        }
        if (EXEC_LSM) {
            auto startTime = chrono::system_clock::now();
            map<int, set<int>> nationSupp;
            for (auto pair: suppNation) {
                int nation = pair.second;
                int supp = pair.first;
                if (nationSupp.count(nation) == 0) {
                    set<int> tmp;
                    nationSupp[nation] = tmp;
                }
                nationSupp[nation].insert(supp);
            }

            char startKeyBuf[LINEITEM_KEY_LEN];
            char endKeyBuf[LINEITEM_KEY_LEN];
            map<int, double> revenue;
            for (auto orderNationPair: orderNation) {
                int orderKey = orderNationPair.first;
                int nationKey = orderNationPair.second;
                sprintf(startKeyBuf, _keyFormat.c_str(), orderKey, 0, 0, 0);
                sprintf(endKeyBuf, _keyFormat.c_str(), orderKey + 1, 0, 0, 0);
                string start(startKeyBuf);
                string end(endKeyBuf);
                rocksdb::Iterator* it = _ins->NewIterator(rocksdb::ReadOptions());
                it->Seek(start);
                while (it->Valid() && it->key().ToString() < end) {
                    string keyString = it->key().ToString();
                    int suppKey = stoi(keyString.substr(14, 5));
                    if (nationSupp[nationKey].count(suppKey) > 0) {
                        string valueString = it->value().ToString();
                        int pos1 = valueString.find('|');
                        int pos2 = valueString.find('|', pos1 + 1);
                        int pos3 = valueString.find('|', pos2 + 1);
                        double price = stod(valueString.substr(pos1 + 1, pos2 - pos1 - 1));
                        double discount = stod(valueString.substr(pos2 + 1, pos3 - pos2 - 1));
                        price = price * (1 - discount);
                        if (revenue.count(nationKey) > 0) {
                            revenue[nationKey] += price;
                        } else {
                            revenue[nationKey] = price;
                        }
                    }
                    it->Next();
                }
                assert(it->status().ok()); // Check for any errors found during the scan
                delete it;
            }
            auto endTime = chrono::system_clock::now();
            auto duration = chrono::duration_cast<chrono::milliseconds>(endTime - startTime);
            cout << "Q5 LSM use time:" << duration.count() << "ms" << endl;
            printResult(revenue, countryName);
        }
        if (EXEC_LSM_GROUP) {
            auto startTime = chrono::system_clock::now();
            map<int, set<int>> nationSupp;
            for (auto pair: suppNation) {
                int nation = pair.second;
                int supp = pair.first;
                if (nationSupp.count(nation) == 0) {
                    set<int> tmp;
                    nationSupp[nation] = tmp;
                }
                nationSupp[nation].insert(supp);
            }

            char startKeyBuf[LINEITEM_KEY_LEN];
            char endKeyBuf[LINEITEM_KEY_LEN];
            map<int, double> revenue;
            for (auto orderNationPair: orderNation) {
                int orderKey = orderNationPair.first;
                int nationKey = orderNationPair.second;
                sprintf(startKeyBuf, _keyFormat.c_str(), orderKey, 0, 0, 0);
                sprintf(endKeyBuf, _keyFormat.c_str(), orderKey + 1, 0, 0, 0);
                string start(startKeyBuf);
                string end(endKeyBuf);
                rocksdb::Iterator* it = _ins->NewIterator(rocksdb::ReadOptions(), _mainGroup);
                it->Seek(start);
                while (it->Valid() && it->key().ToString() < end) {
                    string keyString = it->key().ToString();
                    int suppKey = stoi(keyString.substr(14, 5));
                    if (nationSupp[nationKey].count(suppKey) > 0) {
                        string valueString = it->value().ToString();
                        int pos1 = valueString.find('|');
                        int pos2 = valueString.find('|', pos1 + 1);
                        double price = stod(valueString.substr(0, pos1));
                        double discount = stod(valueString.substr(pos1 + 1, pos2));
                        price = price * (1 - discount);
                        if (revenue.count(nationKey) > 0) {
                            revenue[nationKey] += price;
                        } else {
                            revenue[nationKey] = price;
                        }
                    }
                    it->Next();
                }
                assert(it->status().ok()); // Check for any errors found during the scan
                delete it;
            }
            auto endTime = chrono::system_clock::now();
            auto duration = chrono::duration_cast<chrono::milliseconds>(endTime - startTime);
            cout << "Q5 LSM use time:" << duration.count() << "ms" << endl;
            printResult(revenue, countryName);
        }
    }
}

void Q5::prepareQ5(string region, int year, vector<pair<int, string>>& countryName, vector<pair<int, int>>& suppNation, vector<pair<int, int>>& orderNation) {
    auto startTime = chrono::system_clock::now();
    WT_CURSOR* _cursor = nullptr;
    // get region_key from region by region
    WiredTigerBench::error_check(_COSession->open_cursor(_COSession, "table:region(NAME)", nullptr, nullptr, &_cursor),
                                 "");
    vector<int> validRegionKey;
    char regionName[25];
    memset(regionName, 0, 25);
    memcpy(regionName, region.c_str(), region.size());
    const char* retRegionName;
    int retRegionKey;
    int ret;
    while ((ret = _cursor->next(_cursor)) == 0){
        _cursor->get_key(_cursor, &retRegionKey);
        _cursor->get_value(_cursor, &retRegionName);
        if (strcmp(retRegionName, regionName) == 0) {
            validRegionKey.push_back(retRegionKey);
        }
    }
    if (ret != 0 && ret != WT_NOTFOUND) {
        WiredTigerBench::error_check(ret, "");
    }
    WiredTigerBench::error_check(_cursor->close(_cursor), "");
    // get nationKey from nation by regionKey
    WiredTigerBench::error_check(_COSession->open_cursor(_COSession, "index:nation:region(NATIONKEY,NAME)",
                                                         nullptr, nullptr, &_cursor), "");
    int retNationKey;
    const char* retNationName;
    vector<int> validNationKey;
    for (auto regionKey: validRegionKey){
        _cursor->set_key(_cursor, regionKey);
        WiredTigerBench::error_check((ret = _cursor->search(_cursor)), "");
        while (ret == 0) {
            _cursor->get_key(_cursor, &retRegionKey);
            if (retRegionKey != regionKey) {
                break;
            }
            _cursor->get_value(_cursor, &retNationKey, &retNationName);
            countryName.emplace_back(make_pair(retNationKey, retNationName));
            validNationKey.push_back(retNationKey);
            ret = _cursor->next(_cursor);
        }
        if (ret != 0 && ret != WT_NOTFOUND) {
            WiredTigerBench::error_check(ret, "");
        }
    }
    WiredTigerBench::error_check(_cursor->close(_cursor), "");
    sort(validNationKey.begin(), validNationKey.end());
    // get (supp_key, nation_key)<set> from supplier by nation_key<set>
    WiredTigerBench::error_check(_COSession->open_cursor(_COSession, "index:supplier:nation(SUPPKEY)",
                                                         nullptr, nullptr, &_cursor), "");
    int retSuppKey;
    for (auto nationKey: validNationKey) {
        _cursor->set_key(_cursor, nationKey);
        WiredTigerBench::error_check((ret = _cursor->search(_cursor)), "");
        while (ret == 0) {
            _cursor->get_key(_cursor, &retNationKey);
            if (retNationKey != nationKey) {
                break;
            }
            _cursor->get_value(_cursor, &retSuppKey);
            suppNation.emplace_back(make_pair(retSuppKey, nationKey));
            ret = _cursor->next(_cursor);
        }
        if (ret != 0 && ret != WT_NOTFOUND) {
            WiredTigerBench::error_check(ret, "");
        }
    }
    WiredTigerBench::error_check(_cursor->close(_cursor), "");
    // get (nation_key, cust_key)<set> from customer by nation_key<set>
    map<int, int> validCustNation;
    int retCustKey;
    WiredTigerBench::error_check(_COSession->open_cursor(_COSession, "index:customer:nation(CUSTKEY)",
                                                         nullptr, nullptr, &_cursor), "");
    for (auto nationKey: validNationKey) {
        _cursor->set_key(_cursor, nationKey);
        WiredTigerBench::error_check((ret = _cursor->search(_cursor)), "");
        while (ret == 0) {
            _cursor->get_key(_cursor, &retNationKey);
            if (retNationKey != nationKey) {
                break;
            }
            _cursor->get_value(_cursor, &retCustKey);
            validCustNation[retCustKey] = nationKey;
            ret = _cursor->next(_cursor);
        }
        if (ret != 0 && ret != WT_NOTFOUND) {
            WiredTigerBench::error_check(ret, "");
        }
    }
    WiredTigerBench::error_check(_cursor->close(_cursor), "");
    // get (nation_key, order_key)<set> from orders by cust_key
    WiredTigerBench::error_check(_COSession->open_cursor(_COSession, "table:orders(CUSTKEY,ORDERDATE)",
                                                         nullptr, nullptr, &_cursor), "");
    int retOrderKey;
    const char* retOrderDate;
    while ((ret = _cursor->next(_cursor)) == 0){
        _cursor->get_key(_cursor, &retOrderKey);
        _cursor->get_value(_cursor, &retCustKey, &retOrderDate);
        if (validCustNation.count(retCustKey) > 0) {
            int retYear = (retOrderDate[0] - '0') * 1000 + (retOrderDate[1] - '0') * 100 + (retOrderDate[2] - '0') * 10 +
                    (retOrderDate[3] - '0');
            if (retYear == year) {
                orderNation.emplace_back(make_pair(retOrderKey, validCustNation[retCustKey]));
            }
        }
    }
    if (ret != 0 && ret != WT_NOTFOUND) {
        WiredTigerBench::error_check(ret, "");
    }
    WiredTigerBench::error_check(_cursor->close(_cursor), "");
    // return (nation_key, orderKey)<set>, (nation_key, supp_Key)<set>, (nation_name, nation_key)<map>
    auto endTime = chrono::system_clock::now();
    auto duration = chrono::duration_cast<chrono::milliseconds>(endTime - startTime);
    cout << "prepare use Time:" << duration.count() << "ms, found " << orderNation.size() << " orders and "
        << suppNation.size() << " suppliers." << endl;
}

void Q5::printResult(map<int, double> &revenue, vector<pair<int, string>> &countryName) {
    vector<pair<string, double>> result;
    for (auto pair: revenue) {
        string nation;
        for (int n = 0; n < countryName.size(); n++) {
            if (countryName[n].first == pair.first) {
                nation = countryName[n].second;
                break;
            }
        }
        result.emplace_back(make_pair(nation, pair.second));
    }
    sort(result.begin(), result.end(),
         [](pair<string, double>& l, pair<string, double>& r) { return l.second > r.second;});
    for (auto pair: result){
        cout << pair.first << " " << setprecision(2) << setiosflags(ios::fixed) << pair.second << endl;
    }
}

void Q5::setColumnFamilyHandle(rocksdb::ColumnFamilyHandle* mainGroup, rocksdb::ColumnFamilyHandle* otherGroup) {
    _mainGroup = mainGroup;
    _otherGroup = otherGroup;
}
