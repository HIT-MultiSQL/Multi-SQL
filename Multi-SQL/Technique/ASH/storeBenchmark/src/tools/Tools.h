//
// Created by iron on 2019/11/19.
//

#ifndef STOREBENCHMARK_TOOLS_H
#define STOREBENCHMARK_TOOLS_H

#include <iostream>
#include <vector>
#include <string>

/**
 * 将文件按行进行打乱
 * @param inputDataPath
 * @param outputDataPath
 */
void randomizeFile(const char* inputDataPath, const char* outputDataPath);

class WeightRandom{
public:
    /**
     * @deprecated
     * 加权随机数生成器生成这样的随机数：在[min,max)区间内按照grainSize分为若干个区间，从小到大标号1，2，3...
     * 生成到的随机数在各区间内的概率与它的标号成正比。
     * @param min Inclusive
     * @param max Exclusive
     * @param grainSize
     */
    WeightRandom(int min, int max, int grainSize);
    int nextRandom();
    static void setRandomSeed(unsigned int seed);
private:
    long _factor;
    int _min;
    int _max;
    int _grainSize;
};

/**
 * 获取一个随机的字符串
 * @param valueRange 字符串中字符集的大小
 * @param consecutive  单词长度。生成字符串时，一次会生成一个单词（表现为字面上连续）。
 * @param length 生成的字符串的长度。如果不足完整单词，则会在单词中间进行截断
 * @return
 */
std::string randomString(unsigned int valueRange, unsigned int consecutive, unsigned int length);
std::string randomString(unsigned int length);
extern const char* alphaBet;

#endif //STOREBENCHMARK_TOOLS_H
