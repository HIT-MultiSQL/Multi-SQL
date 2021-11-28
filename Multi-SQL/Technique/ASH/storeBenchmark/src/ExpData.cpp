//
// Created by iron on 2019/12/12.
//

#include "ExpData.h"
#include <fstream>
#include <iostream>
#include <string>

void ExpData::writeToDisk(std::string fileName) {
    std::ofstream out;
    out.open(fileName.c_str());
    if (use_lsm) {
        out << "LSM" << std::endl;
    }
    if (use_bt) {
        out << "BT" << std::endl;
    }
    if (use_column) {
        out << "COL" << std::endl;
    }
    if (use_bt || use_lsm) {
        out << "ValueRange:" << valueRange << ", Consecutive:" << valueConsecutive
            << ", RedundancyBit:" << actualRedundantBit << ", RowSize:" << rowSize
            << ", KeyField:" << keyField << ", ValueField:" << valueField << std::endl;
    }
    if (use_column) {
        out << "ValueRange:" << valueRange << ", Consecutive:" << valueConsecutive
            << ", RedundancyBit:" << actualRedundantBit << ", RowSize:" << rowSize
            << ", IntField:" << intField << ", StringField:" << strField << std::endl;
    }

    auto batchIt = batchDatas.begin();
    out << "timeStampMill" << ","
        << "batchID" << ","
        << "tableSize" << ","
        << "log_totalReadRows" << ","
        << "latest10Writes" << ","
        << "latest30Writes" << ","
        << "latest60Writes" << ","
        << "lsm_l1" << ","
        << "lsm_l2" << ","
        << "lsm_levels" << ","
        << "deltaCachePage" << ","
        << "cacheRatio" << ","
        << "keyVariance" << ","
        << "htg_scanRatio" << ","
        << "htg_updateRatio" << ","
        << "htg_updateRatioAvg" << ","
        << "readRows" << ","
        << "writeRows" << ","
        << "currentRows" << ","
        << "type" << ","
        << "useMicroTimeAvg" << ","
        << "useTimeMicro"
        << std::endl;
    for(auto & record : records) {
        while (record->batchID > (*batchIt)->batchID) {
            batchIt++;
            if (batchIt == batchDatas.end()) {
                std::cout << "ERROR in ExpData::writeToDisk batch iterator meet end" << std::endl;
                break;
            }
        }
        if (record->batchID != (*batchIt)->batchID) {
            std::cout << "ERROR in ExpData::writeToDisk lack batch info" << std::endl;
            break;
        }
        out << std::to_string(record->timeStampNano / 1000000) << ","
            << std::to_string((*batchIt)->batchID) << ","
            << std::to_string((*batchIt)->tableSize) << ","
            << std::to_string((*batchIt)->log_totalReadRows) << ","
            << std::to_string((*batchIt)->latest10Writes) << ","
            << std::to_string((*batchIt)->latest30Writes) << ","
            << std::to_string((*batchIt)->latest60Writes) << ","
            << std::to_string((*batchIt)->lsm_l1) << ","
            << std::to_string((*batchIt)->lsm_l2) << ","
            << std::to_string((*batchIt)->lsm_levels) << ","
            << std::to_string((*batchIt)->deltaCachePage) << ","
            << std::to_string((*batchIt)->cacheRatio) << ","
            << std::to_string((*batchIt)->keyVariance) << ","
            << std::to_string((*batchIt)->htg_scanRatio) << ","
            << std::to_string((*batchIt)->htg_updateRatio) << ","
            << std::to_string((*batchIt)->htg_updateRatioAvg) << ","
            << std::to_string(record->readRows) << ","
            << std::to_string(record->writeRows) << ","
            << std::to_string(record->currentRows) << ","
            << std::to_string(record->type) << ","
            << std::to_string((*batchIt)->useMicroTimeAvg) << ","
            << std::to_string(record->useTimeMicro)
            << std::endl;
    }
    out.close();
}

void ExpData::printExpInfo(std::string title) {
    std::cout << title << std::endl;
    if (use_lsm) {
        std::cout << "LSM" << std::endl;
    }
    if (use_bt) {
        std::cout << "BT" << std::endl;
    }
    if (use_column) {
        std::cout << "COL" << std::endl;
    }
    if (use_bt || use_lsm) {
        std::cout << "ValueRange:" << valueRange << ", Consecutive:" << valueConsecutive
            << ", RedundancyBit:" << actualRedundantBit << ", RowSize:" << rowSize
            << ", KeyField:" << keyField << ", ValueField:" << valueField << std::endl;
    }
    if (use_column) {
        std::cout << "ValueRange:" << valueRange << ", Consecutive:" << valueConsecutive
            << ", RedundancyBit:" << actualRedundantBit << ", RowSize:" << rowSize
            << ", IntField:" << intField << ", StringField:" << strField << std::endl;
    }
}
