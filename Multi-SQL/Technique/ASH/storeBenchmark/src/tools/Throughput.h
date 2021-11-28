//
// Created by iron on 2019/12/11.
//

#ifndef STOREBENCHMARK_THROUGHPUT_H
#define STOREBENCHMARK_THROUGHPUT_H

class Throughput {
public:
    /**
     * 近一段时间的吞吐量的计算工具。本质上是一个循环队列， 可以
     * 1. 对当前时间对应的窗口进行累加操作
     * 2. 对最近的N个窗口求和
     * 一个窗口的时间为1s
     * @param duration 缓存的窗口个数
     */
    explicit Throughput(int duration);
    long latestNAverage(unsigned int duration);
    long latestNAverage(unsigned int duration, long baseTS);
    void record(long bytes);
    void print();
    ~Throughput();
private:
    void shift(int pos, long minTS);
    long _latestTS;
    int _latestPos;
    long* _data;
    int _duration;
};

void throughputTest();
#endif //STOREBENCHMARK_THROUGHPUT_H
