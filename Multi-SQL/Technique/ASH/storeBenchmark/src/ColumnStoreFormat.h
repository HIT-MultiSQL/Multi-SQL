//
// Created by ironwei on 2020/3/20.
//

#ifndef STOREBENCHMARK_COLUMNSTOREFORMAT_H
#define STOREBENCHMARK_COLUMNSTOREFORMAT_H

#include <string>
#include <vector>
#include "wiredtiger.h"

#define FORMAT_TYPE_SIZE 14

using namespace std;
class ColumnStoreFormat {
public:
    explicit ColumnStoreFormat(int intFields, int strField): _intFields(intFields), _strFields(strField) {}
    string getFormat() { return _format;}
    void (*pWTSetValue)(WT_CURSOR*, std::vector<int>&, std::vector<string>&) = nullptr;
    int getIntFields() { return _intFields;}
    int getStrFields() { return _strFields;}
    string _format;
private:
    int _intFields;
    int _strFields;
};

class I1Format: public ColumnStoreFormat {
public:
    explicit I1Format(): ColumnStoreFormat(1, 0) {
        _format = "II";
        pWTSetValue = wtSetValue;
    }
    static void wtSetValue(WT_CURSOR* cursor, std::vector<int>& intValue, std::vector<string>& strValue) {
        cursor->set_value(cursor, intValue[0], intValue[1]);
    }
};

class S1Format: public ColumnStoreFormat {
public:
    explicit S1Format(): ColumnStoreFormat(0, 1) {
        _format = "IS";
        pWTSetValue = wtSetValue;
    }
    static void wtSetValue(WT_CURSOR* cursor, std::vector<int>& intValue, std::vector<string>& strValue) {
        cursor->set_value(cursor, intValue[0], strValue[0].c_str());
    }
};

class I2Format: public ColumnStoreFormat {
public:
    explicit I2Format(): ColumnStoreFormat(2, 0) {
        _format = "III";
        pWTSetValue = wtSetValue;
    }
    static void wtSetValue(WT_CURSOR* cursor, std::vector<int>& intValue, std::vector<string>& strValue) {
        cursor->set_value(cursor, intValue[0], intValue[1], intValue[2]);
    }
};

class I1S1Format: public ColumnStoreFormat {
public:
    explicit I1S1Format(): ColumnStoreFormat(1, 1) {
        _format = "IIS";
        pWTSetValue = wtSetValue;
    }
    static void wtSetValue(WT_CURSOR* cursor, std::vector<int>& intValue, std::vector<string>& strValue) {
        cursor->set_value(cursor, intValue[0], intValue[1], strValue[0].c_str());
    }
};

class S2Format: public ColumnStoreFormat {
public:
    explicit S2Format(): ColumnStoreFormat(0, 2) {
        _format = "ISS";
        pWTSetValue = wtSetValue;
    }
    static void wtSetValue(WT_CURSOR* cursor, std::vector<int>& intValue, std::vector<string>& strValue) {
        cursor->set_value(cursor, intValue[0], strValue[0].c_str(), strValue[1].c_str());
    }
};

class I3Format: public ColumnStoreFormat {
public:
    explicit I3Format(): ColumnStoreFormat(3, 0) {
        _format = "IIII";
        pWTSetValue = wtSetValue;
    }
    static void wtSetValue(WT_CURSOR* cursor, std::vector<int>& intValue, std::vector<string>& strValue) {
        cursor->set_value(cursor, intValue[0], intValue[1], intValue[2], intValue[3]);
    }
};

class I1S2Format: public ColumnStoreFormat {
public:
    explicit I1S2Format(): ColumnStoreFormat(1, 2) {
        _format = "IISS";
        pWTSetValue = wtSetValue;
    }
    static void wtSetValue(WT_CURSOR* cursor, std::vector<int>& intValue, std::vector<string>& strValue) {
        cursor->set_value(cursor, intValue[0], intValue[1], strValue[0].c_str(), strValue[1].c_str());
    }
};

class S3Format: public ColumnStoreFormat {
public:
    explicit S3Format(): ColumnStoreFormat(0, 3) {
        _format = "ISSS";
        pWTSetValue = wtSetValue;
    }
    static void wtSetValue(WT_CURSOR* cursor, std::vector<int>& intValue, std::vector<string>& strValue) {
        cursor->set_value(cursor, intValue[0], strValue[0].c_str(), strValue[1].c_str(), strValue[2].c_str());
    }
};

class I1S3Format: public ColumnStoreFormat {
public:
    explicit I1S3Format(): ColumnStoreFormat(1, 3) {
        _format = "IISSS";
        pWTSetValue = wtSetValue;
    }
    static void wtSetValue(WT_CURSOR* cursor, std::vector<int>& intValue, std::vector<string>& strValue) {
        cursor->set_value(cursor, intValue[0], intValue[1], strValue[0].c_str(), strValue[1].c_str(), strValue[2].c_str());
    }
};

class I2S3Format: public ColumnStoreFormat {
public:
    explicit I2S3Format(): ColumnStoreFormat(2, 3) {
        _format = "IIISSS";
        pWTSetValue = wtSetValue;
    }
    static void wtSetValue(WT_CURSOR* cursor, std::vector<int>& intValue, std::vector<string>& strValue) {
        cursor->set_value(cursor, intValue[0], intValue[1], intValue[2],
                strValue[0].c_str(), strValue[1].c_str(), strValue[2].c_str());
    }
};

class I2S5Format: public ColumnStoreFormat {
public:
    explicit I2S5Format(): ColumnStoreFormat(2, 5) {
        _format = "IIISSSSS";
        pWTSetValue = wtSetValue;
    }
    static void wtSetValue(WT_CURSOR* cursor, std::vector<int>& intValue, std::vector<string>& strValue) {
        cursor->set_value(cursor, intValue[0], intValue[1], intValue[2],
                          strValue[0].c_str(), strValue[1].c_str(), strValue[2].c_str(),
                          strValue[3].c_str(), strValue[4].c_str());
    }
};

class I2S8Format: public ColumnStoreFormat {
public:
    explicit I2S8Format(): ColumnStoreFormat(2, 8) {
        _format = "IIISSSSSSSS";
        pWTSetValue = wtSetValue;
    }
    static void wtSetValue(WT_CURSOR* cursor, std::vector<int>& intValue, std::vector<string>& strValue) {
        cursor->set_value(cursor, intValue[0], intValue[1], intValue[2],
                          strValue[0].c_str(), strValue[1].c_str(), strValue[2].c_str(),
                          strValue[3].c_str(), strValue[4].c_str(), strValue[5].c_str(),
                          strValue[6].c_str(), strValue[7].c_str());
    }
};

class I5S8Format: public ColumnStoreFormat {
public:
    explicit I5S8Format(): ColumnStoreFormat(5, 8){
        _format = "IIIIIISSSSSSSS";
        pWTSetValue = wtSetValue;
    }
    static void wtSetValue(WT_CURSOR* cursor, std::vector<int>& intValue, std::vector<string>& strValue) {
        cursor->set_value(cursor, intValue[0], intValue[1], intValue[2], intValue[3], intValue[4], intValue[5],
                          strValue[0].c_str(), strValue[1].c_str(), strValue[2].c_str(),
                          strValue[3].c_str(), strValue[4].c_str(), strValue[5].c_str(),
                          strValue[6].c_str(), strValue[7].c_str());
    }
};

class I5S12Format: public ColumnStoreFormat {
public:
    explicit I5S12Format(): ColumnStoreFormat(5, 12){
        _format = "IIIIIISSSSSSSSSSSS";
        pWTSetValue = wtSetValue;
    }
    static void wtSetValue(WT_CURSOR* cursor, std::vector<int>& intValue, std::vector<string>& strValue) {
        cursor->set_value(cursor, intValue[0], intValue[1], intValue[2], intValue[3], intValue[4], intValue[5],
                          strValue[0].c_str(), strValue[1].c_str(), strValue[2].c_str(),
                          strValue[3].c_str(), strValue[4].c_str(), strValue[5].c_str(),
                          strValue[6].c_str(), strValue[7].c_str(), strValue[8].c_str(),
                          strValue[9].c_str(), strValue[10].c_str(), strValue[11].c_str());
    }
};

class UserInformFormat: public ColumnStoreFormat {
public:
    explicit UserInformFormat(): ColumnStoreFormat(1, 5){
        _format = "ISB1sSS";
        pWTSetValue = nullptr;
    }
};

class UserBehaviorFormat: public ColumnStoreFormat {
public:
    explicit UserBehaviorFormat(): ColumnStoreFormat(1, 4){
        _format = "IISSS";
        pWTSetValue = nullptr;
    }
};

class LineItemFormat: public ColumnStoreFormat {
public:
    explicit LineItemFormat(): ColumnStoreFormat(5, 11){
        _format = "IIIHHSSS1s1sSSSSSS";
        pWTSetValue = nullptr;
    }
};


#endif //STOREBENCHMARK_COLUMNSTOREFORMAT_H
