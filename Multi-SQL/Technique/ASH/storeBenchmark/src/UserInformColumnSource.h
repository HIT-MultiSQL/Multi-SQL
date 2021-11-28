//
// Created by yanhao on 2020/7/31.
//

#ifndef STOREBENCHMARK_USERINFORMCOLUMNSOURCE_H
#define STOREBENCHMARK_USERINFORMCOLUMNSOURCE_H
#include <set>
#include "DataSource.h"
#include "ColumnStoreFormat.h"
#include "ColumnStoreSource.h"

class UserInformColumnSource: public ColumnStoreSource {
public:
    explicit UserInformColumnSource(bool isRandom, string& filePath): ColumnStoreSource(isRandom, 1, 10000),
                                                                    _filePath(filePath){

    };
protected:
    void setTargetFormat(long (*pWTGetLeadKey)(WT_CURSOR*)) override {};
    // full names(include myid and leadkey)
    string getWTColumnNames() const override {return "MYID,ID,NAME,AGE,GENDER,OCCUPATION,REGISTER_DATE";};
    // value names
    vector<string> getFields() override;
    string getLeadKeyField() override{return "ID"; };
    // init vector
    void initValuePool(int rowSize, int avgRange, int avgConsecutive) override;
    void wtSetNextValue(WT_CURSOR* cursor) override;
    long wtGetCursorLeadKey(WT_CURSOR* cursor) const override;
    void wtInitLeadKeyCursor(WT_CURSOR* cursor, long leadKey) const override;

private:
    string _filePath;
    std::vector<std::string> _lines;
};
#endif //STOREBENCHMARK_USERINFORMCOLUMNSOURCE_H
