//
// Created by iron on 2019/12/3.
//

#ifndef STOREBENCHMARK_LINEITEM_H
#define STOREBENCHMARK_LINEITEM_H

#include <string>
#include "dataDesc.h"

using namespace std;
class LineItem {
public:
    int getOrderKey() const;
    void setOrderKey(int orderKey);
    int getPartKey() const;
    void setPartKey(int partKey);
    int getSuppKey() const;
    void setSuppKey(int suppKey);
    int getLineNumber() const;
    void setLineNumber(int lineNumber);
    int getQuantity() const;
    void setQuantity(int quantity);
    const std::string &getExtendPrice() const;
    void setExtendPrice(const std::string &extendPrice);
    const std::string &getDiscount() const;
    void setDiscount(const std::string &discount);
    const std::string &getTax() const;
    void setTax(const std::string &tax);
    char getReturnFlag() const;
    void setReturnFlag(char returnFlag);
    char getLineStatus() const;
    void setLineStatus(char lineStatus);
    const std::string &getShipDate() const;
    void setShipDate(const std::string &shipDate);
    const std::string &getCommitDate() const;
    void setCommitDate(const std::string &commitDate);
    const std::string &getReceiptDate() const;
    void setReceiptDate(const std::string &receiptDate);
    const std::string &getShipInstruct() const;
    void setShipInstruct(const std::string &shipInstruct);
    const std::string &getShipMode() const;
    void setShipMode(const std::string &shipMode);
    const std::string &getComment() const;
    void setComment(const std::string &comment);
private:
    int _orderKey = 0;
    int _partKey = 0;
    int _suppKey = 0;
    int _lineNumber = 0;
    int _quantity = 0;
    std::string _extendPrice;
    std::string _discount;
    std::string _tax;
    char _returnFlag = '\0';
    char _lineStatus = '\0';
    std::string _shipDate;
    std::string _commitDate;
    std::string _receiptDate;
    std::string _shipInstruct;
    std::string _shipMode;
    std::string _comment;
};

struct LineItemStruct {
    uint32_t orderKey = 0;
    uint32_t partKey = 0;
    uint32_t suppKey = 0;
    uint16_t lineNumber = 0;
    uint16_t quantity = 0;
    char* extendPrice = nullptr;
    char* discount = nullptr;
    char* tax = nullptr;
    char* returnFlag = new char;
    char* lineStatus = new char;
    char* shipDate = nullptr;
    char* commitDate = nullptr;
    char* receiptDate = nullptr;
    char* shipInstruct = nullptr;
    char* shipMode = nullptr;
    char* comment = nullptr;
    ~LineItemStruct() {
        delete[] extendPrice;
        delete[] discount;
        delete[] tax;
        delete returnFlag;
        delete lineStatus;
        delete[] shipDate;
        delete[] commitDate;
        delete[] receiptDate;
        delete[] shipInstruct;
        delete[] shipMode;
        delete[] comment;
    }
};


#endif //STOREBENCHMARK_LINEITEM_H
