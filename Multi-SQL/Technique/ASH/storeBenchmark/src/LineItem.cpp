//
// Created by iron on 2019/12/3.
//

#include "LineItem.h"
int LineItem::getOrderKey() const {
    return _orderKey;
}

void LineItem::setOrderKey(int orderKey) {
    _orderKey = orderKey;
}

int LineItem::getPartKey() const {
    return _partKey;
}

void LineItem::setPartKey(int partKey) {
    _partKey = partKey;
}

int LineItem::getSuppKey() const {
    return _suppKey;
}

void LineItem::setSuppKey(int suppKey) {
    _suppKey = suppKey;
}

int LineItem::getLineNumber() const {
    return _lineNumber;
}

void LineItem::setLineNumber(int lineNumber) {
    _lineNumber = lineNumber;
}

int LineItem::getQuantity() const {
    return _quantity;
}

void LineItem::setQuantity(int quantity) {
    _quantity = quantity;
}

const std::string &LineItem::getExtendPrice() const {
    return _extendPrice;
}

void LineItem::setExtendPrice(const std::string &extendPrice) {
    _extendPrice = extendPrice;
}

const std::string &LineItem::getDiscount() const {
    return _discount;
}

void LineItem::setDiscount(const std::string &discount) {
    _discount = discount;
}

const std::string &LineItem::getTax() const {
    return _tax;
}

void LineItem::setTax(const std::string &tax) {
    _tax = tax;
}

char LineItem::getReturnFlag() const {
    return _returnFlag;
}

void LineItem::setReturnFlag(char returnFlag) {
    _returnFlag = returnFlag;
}

char LineItem::getLineStatus() const {
    return _lineStatus;
}

void LineItem::setLineStatus(char lineStatus) {
    _lineStatus = lineStatus;
}

const std::string &LineItem::getShipDate() const {
    return _shipDate;
}

void LineItem::setShipDate(const std::string &shipDate) {
    _shipDate = shipDate;
}

const std::string &LineItem::getCommitDate() const {
    return _commitDate;
}

void LineItem::setCommitDate(const std::string &commitDate) {
    _commitDate = commitDate;
}

const std::string &LineItem::getReceiptDate() const {
    return _receiptDate;
}

void LineItem::setReceiptDate(const std::string &receiptDate) {
    _receiptDate = receiptDate;
}

const std::string &LineItem::getShipInstruct() const {
    return _shipInstruct;
}

void LineItem::setShipInstruct(const std::string &shipInstruct) {
    _shipInstruct = shipInstruct;
}

const std::string &LineItem::getShipMode() const {
    return _shipMode;
}

void LineItem::setShipMode(const std::string &shipMode) {
    _shipMode = shipMode;
}

const std::string &LineItem::getComment() const {
    return _comment;
}

void LineItem::setComment(const std::string &comment) {
    _comment = comment;
}