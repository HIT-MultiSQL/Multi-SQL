//
// Created by iron on 2020/1/12.
//

#include "DataSource.h"
#include "WiredTigerBench.h"
#include <fstream>
#include <cstring>
#include <sstream>

LineItemSource::LineItemSource(const char *filePath) {
    _path = filePath;
    std::fstream _fstream;
    _fstream.open(_path);
    while (_fstream.getline(_buffer, BUFFER_LEN)) {
        if (strlen(_buffer) > 0) {
            std::string line(_buffer);
            _lines.push_back(line);
        }
    }
    _fstream.close();

    stringstream sb1;
    sb1 << "%0" << ORDERKEY_LEN << "d%0" << PARTKEY_LEN << "d%0"
        << SUPPKEY_LEN << "d%0" << LINENUMBER_LEN <<"d";
    _keyFormat = sb1.str();
}

LineItem *LineItemSource::parseLineItem(const std::string &line) {
    auto* ret = new LineItem();
    unsigned int pos = 0;
    // get 1st
    size_t fieldStop = line.find('|', pos);
    std::string field = line.substr(pos, fieldStop - pos);
    ret->setOrderKey(stoi(field));
    pos = fieldStop + 1;
    // get 2nd
    fieldStop = line.find('|', pos);
    field = line.substr(pos, fieldStop - pos);
    ret->setPartKey(stoi(field));
    pos = fieldStop + 1;
    // get 3rd
    fieldStop = line.find('|', pos);
    field = line.substr(pos, fieldStop - pos);
    ret->setSuppKey(stoi(field));
    pos = fieldStop + 1;
    // get 4th
    fieldStop = line.find('|', pos);
    field = line.substr(pos, fieldStop - pos);
    ret->setLineNumber(stoi(field));
    pos = fieldStop + 1;
    // get 5th
    fieldStop = line.find('|', pos);
    field = line.substr(pos, fieldStop - pos);
    ret->setQuantity(stoi(field));
    pos = fieldStop + 1;
    // get 6th
    fieldStop = line.find('|', pos);
    ret->setExtendPrice(line.substr(pos, fieldStop - pos));
    pos = fieldStop + 1;
    // get 7th
    fieldStop = line.find('|', pos);
    ret->setDiscount(line.substr(pos, fieldStop - pos));
    pos = fieldStop + 1;
    // get 8th
    fieldStop = line.find('|', pos);
    ret->setTax(line.substr(pos, fieldStop - pos));
    pos = fieldStop + 1;
    // get 9th
    ret->setReturnFlag(line[pos]);
    pos = pos + 2;
    // get 10th
    ret->setLineStatus(line[pos]);
    pos = pos + 2;
    // get 11th
    fieldStop = line.find('|', pos);
    ret->setShipDate(line.substr(pos, fieldStop - pos));
    pos = fieldStop + 1;
    // get 12th
    fieldStop = line.find('|', pos);
    ret->setCommitDate(line.substr(pos, fieldStop - pos));
    pos = fieldStop + 1;
    // get 13th
    fieldStop = line.find('|', pos);
    ret->setReceiptDate(line.substr(pos, fieldStop - pos));
    pos = fieldStop + 1;
    // get 14th
    fieldStop = line.find('|', pos);
    ret->setShipInstruct(line.substr(pos, fieldStop - pos));
    pos = fieldStop + 1;
    // get 15th
    fieldStop = line.find('|', pos);
    ret->setShipMode(line.substr(pos, fieldStop - pos));
    pos = fieldStop + 1;
    // get 16th
    fieldStop = line.find('|', pos);
    ret->setComment(line.substr(pos, fieldStop - pos));
    return ret;
}

LineItemStruct *LineItemSource::parseLineItemStruct(const std::string &line) {
    auto* ret = new LineItemStruct();
    unsigned int pos = 0;
    // get 1st
    size_t fieldStop = line.find('|', pos);
    std::string field = line.substr(pos, fieldStop - pos);
    ret->orderKey = stoi(field);
    pos = fieldStop + 1;
    // get 2nd
    fieldStop = line.find('|', pos);
    field = line.substr(pos, fieldStop - pos);
    ret->partKey = stoi(field);
    pos = fieldStop + 1;
    // get 3rd
    fieldStop = line.find('|', pos);
    field = line.substr(pos, fieldStop - pos);
    ret->suppKey = stoi(field);
    pos = fieldStop + 1;
    // get 4th
    fieldStop = line.find('|', pos);
    field = line.substr(pos, fieldStop - pos);
    ret->lineNumber = stoi(field);
    pos = fieldStop + 1;
    // get 5th
    fieldStop = line.find('|', pos);
    field = line.substr(pos, fieldStop - pos);
    ret->quantity = stoi(field);
    pos = fieldStop + 1;
    // get 6th
    fieldStop = line.find('|', pos);
    ret->extendPrice = new char[fieldStop - pos + 1];
    strcpy(ret->extendPrice, line.substr(pos, fieldStop - pos).data());
    pos = fieldStop + 1;
    // get 7th
    fieldStop = line.find('|', pos);
    ret->discount = new char[fieldStop - pos + 1];
    strcpy(ret->discount, line.substr(pos, fieldStop - pos).data());
    pos = fieldStop + 1;
    // get 8th
    fieldStop = line.find('|', pos);
    ret->tax = new char[fieldStop - pos + 1];
    strcpy(ret->tax, line.substr(pos, fieldStop - pos).data());
    pos = fieldStop + 1;
    // get 9th
    *(ret->returnFlag) = line[pos];
    pos = pos + 2;
    // get 10th
    *(ret->lineStatus) = line[pos];
    pos = pos + 2;
    // get 11th
    fieldStop = line.find('|', pos);
    ret->shipDate = new char[fieldStop - pos + 1];
    strcpy(ret->shipDate, line.substr(pos, fieldStop - pos).data());
    pos = fieldStop + 1;
    // get 12th
    fieldStop = line.find('|', pos);
    ret->commitDate = new char[fieldStop - pos + 1];
    strcpy(ret->commitDate, line.substr(pos, fieldStop - pos).data());
    pos = fieldStop + 1;
    // get 13th
    fieldStop = line.find('|', pos);
    ret->receiptDate = new char[fieldStop - pos + 1];
    strcpy(ret->receiptDate, line.substr(pos, fieldStop - pos).data());
    pos = fieldStop + 1;
    // get 14th
    fieldStop = line.find('|', pos);
    ret->shipInstruct = new char[fieldStop - pos + 1];
    strcpy(ret->shipInstruct, line.substr(pos, fieldStop - pos).data());
    pos = fieldStop + 1;
    // get 15th
    fieldStop = line.find('|', pos);
    ret->shipMode = new char[fieldStop - pos + 1];
    strcpy(ret->shipMode, line.substr(pos, fieldStop - pos).data());
    pos = fieldStop + 1;
    // get 16th
    fieldStop = line.find('|', pos);
    ret->comment = new char[fieldStop - pos + 1];
    strcpy(ret->comment, line.substr(pos, fieldStop - pos).data());
    return ret;
}

bool LineItemSource::hasNext() const {
    return _pos < _lines.size();
}

void LineItemSource::close() {}

LineItem *LineItemSource::next() {
    std::string line = _lines[_pos];
    LineItem * ret = parseLineItem(line);
    _pos ++;
    return ret;
}

LineItemStruct *LineItemSource::nextItemStruct() {
    std::string line = _lines[_pos];
    LineItemStruct * ret = parseLineItemStruct(line);
    _pos ++;
    return ret;
}

string LineItemSource::getNextKeyString() {
    delete _currentItem;
    _currentItem = next();
    char key[LINEITEM_KEY_LEN];
    sprintf(key, _keyFormat.data(), _currentItem->getOrderKey(), _currentItem->getPartKey(),
            _currentItem->getSuppKey(), _currentItem->getLineNumber());
    string ret(key);
    _currentLeadKey = _currentItem->getOrderKey();
    return ret;
}

long LineItemSource::wtSetNextKey(WT_CURSOR* cursor) {
    delete _currentItemStruct;
    _currentItemStruct = nextItemStruct();
    LineItemStruct* item = _currentItemStruct;
    cursor->set_key(cursor,item->orderKey, item->partKey, item->suppKey, item->lineNumber);
    _currentLeadKey = item->orderKey;
    return item->orderKey;
}

string LineItemSource::getNextValueString() {
    LineItem* item = _currentItem;
    char buf[4];
    sprintf(buf, "%02d", item->getQuantity());
    stringstream sb;
    sb << buf;
    int padding = EXTENDPRICE_LEN - item->getExtendPrice().length();
    for (int i = 0; i < padding; i++) {
        sb << '0';
    }
    sb << item->getExtendPrice();
    padding = 4 - item->getDiscount().length();
    for (int i = 0; i < padding; i++) {
        sb << '0';
    }
    sb << item->getDiscount();
    padding = 4 - item->getTax().length();
    for (int i = 0; i < padding; i++) {
        sb << '0';
    }
    sb << item->getTax() << item->getReturnFlag() << item->getLineStatus() << item->getShipDate()
       << item->getCommitDate() << item->getReceiptDate();
    padding = SHIPINSTRUCT_LEN - item->getShipInstruct().length();
    for (int i = 0; i < padding; i++) {
        sb << '#';
    }
    sb << item->getShipInstruct();
    padding = SHIPMODE_LEN - item->getShipMode().length();
    for (int i = 0; i < padding; i++) {
        sb << '#';
    }
    sb << item->getShipMode();
    padding = COMMENT_LEN - item->getComment().length();
    for (int i = 0; i < padding; i++) {
        sb << '#';
    }
    sb << item->getComment();
    return sb.str();
}

void LineItemSource::wtSetNextValue(WT_CURSOR* cursor) {
    LineItemStruct* item = _currentItemStruct;
    cursor->set_value(cursor,
                       item->quantity, item->extendPrice, item->discount, item->tax,
                       item->returnFlag, item->lineStatus, item->shipDate, item->commitDate,
                       item->receiptDate, item->shipInstruct, item->shipMode, item->comment);

}

long LineItemSource::wtGetCursorLeadKey(WT_CURSOR* cursor) const {
    int orderKey;
    int partKey;
    int suppKey;
    int lineNumber;
    WiredTigerBench::error_check(cursor->get_key(cursor, &orderKey, &partKey, &suppKey, &lineNumber), "LineItemSource::wtGetCursorLeadKey");
    return orderKey;
}

void LineItemSource::wtInitLeadKeyCursor(WT_CURSOR *cursor, long leadKey) const {
    cursor->set_key(cursor, leadKey, 0, 0, 0);
}

string LineItemSource::initLeadKeyString(long leadKey) const {
    char leadKeyBuf[LINEITEM_KEY_LEN];
    sprintf(leadKeyBuf, _keyFormat.data(), leadKey, 0, 0, 0);
    return std::string(leadKeyBuf);
}

long LineItemSource::parseLeadKeyFromString(string keyString) const {
    return stoi(keyString.substr(0, ORDERKEY_LEN));
}

LineItemReadOnlySource::LineItemReadOnlySource() {
    stringstream sb;
    sb << "%0" << ORDERKEY_LEN << "d%0" << PARTKEY_LEN << "d%0"
        << SUPPKEY_LEN << "d%0" << LINENUMBER_LEN <<"d";
    _keyFormat = sb.str();
}

string LineItemReadOnlySource::initLeadKeyString(long leadKey) const {
    char leadKeyBuf[LINEITEM_KEY_LEN];
    sprintf(leadKeyBuf, _keyFormat.data(), leadKey, 0, 0, 0);
    return std::string(leadKeyBuf);
}

long LineItemReadOnlySource::wtGetCursorLeadKey(WT_CURSOR *cursor) const {
    int orderKey;
    int partKey;
    int suppKey;
    int lineNumber;
    WiredTigerBench::error_check(cursor->get_key(cursor, &orderKey, &partKey, &suppKey, &lineNumber), "LineItemReadOnlySource::wtGetCursorLeadKey");
    return orderKey;
}

long LineItemReadOnlySource::parseLeadKeyFromString(string keyString) const {
    return stoi(keyString.substr(0, ORDERKEY_LEN));
}

void LineItemReadOnlySource::wtInitLeadKeyCursor(WT_CURSOR *cursor, long leadKey) const {
    cursor->set_key(cursor, leadKey, 0, 0, 0);
}
