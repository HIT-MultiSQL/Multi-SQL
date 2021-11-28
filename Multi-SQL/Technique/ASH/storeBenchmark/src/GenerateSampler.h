//
// Created by iron on 2020/1/13.
//

#ifndef STOREBENCHMARK_GENERATESAMPLER_H
#define STOREBENCHMARK_GENERATESAMPLER_H

#include <string>
#include "TestBench.h"
#include "KeySource.h"
#include "ValueSource.h"
#include "ColumnStoreFormat.h"


using namespace std;
/**
 * 生成采样数据点并执行采样
 */
class GenerateSampler {
public:
    static void runExperiment(string& path, TestStyle style, int serialNum);
    static void runColumnExp(string& path, TestStyle style, int serialNum);
    static void runTPCColumnExp(string &lineItemPath, string &path, TestStyle style, int serialNum);
    static void runColumnFCExp(string& path, int start, int end);
    static void runTPC(string &lineItemPath, string &path, TestStyle style, int serialNum);
    static void waitUntilNextFlush();
    static void getFormatFromText(int serialNum, int& retIntNum, int& retStrNum);
    static void runUserInform(string &userInformPath, string &path, TestStyle style, int serialNum);
    static void runUserBehavior(string &userBehaviorPath, string &path, TestStyle style, int serialNum);
    static void runUserInformColumnExp(string &userInformPath, string &path, TestStyle style, int serialNum);
    static void runUserBehaviorColumnExp(string &userBehaviorPath, string &path, TestStyle style, int serialNum);

protected:
    static ColumnStoreFormat* initColumnFormat();
    static KeySource* getKeySource(unsigned int& rowLength);
    static ValueSource* getValueSource(unsigned int length, unsigned int valueRange, unsigned int consecutive);
};


#endif //STOREBENCHMARK_GENERATESAMPLER_H
