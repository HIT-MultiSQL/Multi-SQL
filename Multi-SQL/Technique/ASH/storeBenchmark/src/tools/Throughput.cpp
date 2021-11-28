//
// Created by iron on 2019/12/11.
//

#include <iostream>
#include "Throughput.h"
#include <ctime>
#include <thread>
#include <chrono>

Throughput::Throughput(int duration):_duration(duration) {
    _data = new long[duration];
    for (int i = 0; i < duration; i++) {
        _data[i] = 0;
    }
    _latestTS = time(0);
    _latestPos = 0;
}

Throughput::~Throughput() {
    delete[] _data;
}

void throughputTest() {
    Throughput t(10);
    for (int i = 0; i < 1000; i++) {
        int b = random() % 1000;
        t.record(b);
        std::cout << time(0) << " : " << b << std::endl;
        if (i % 10 == 0) {
            t.print();
            std::cout << "latest 3 avg " << t.latestNAverage(3) << std::endl;
        }
        int s = random() % 21;
        if (s == 7) {
            std::this_thread::sleep_for (std::chrono::seconds(11));
        } else {
            std::this_thread::sleep_for (std::chrono::milliseconds(100 * s));
        }
    }
}

long Throughput::latestNAverage(unsigned int duration) {
    return latestNAverage(duration, time(0));
}

void Throughput::record(long bytes) {
    long now = time(0);
    if (now - _latestTS >= _duration) {
        int maxTS = _latestTS + _duration - 1;
        shift(now - maxTS, now);
    }
    _data[(_latestPos + (now - _latestTS)) % _duration] += bytes;
}

long Throughput::latestNAverage(unsigned int duration, long baseTS) {
    if (duration == 0) return 0;
    // 保证baseTS >= latestPos, baseTS - duration >= latestTS - 1
    if (baseTS < _latestPos) return 0;
    if (baseTS - duration + 1 < _latestTS) {
        duration = baseTS - _latestTS + 1;
    }

    long maxTS = _latestTS + _duration - 1;
    long sum = 0;
    if (baseTS > maxTS) {
        if (baseTS - maxTS >= duration) { // case1: 查询时间段[baseTS-duration+1, baseTS]与存储的数据没有重叠
            return 0;
        } else { // case2: minTS <= baseTS-duration+1 <= maxTS < baseTS
            int fromPos = baseTS - duration + 1 - _latestTS;
            for (int i = _latestPos + fromPos; i < _latestPos + _duration; i++) {
                sum += _data[i % _duration];
            }
            return sum / duration;
        }
    } else { // case3: minTS <= baseTS-duration+1 <= baseTS <= maxTS
        int toPos = baseTS - _latestTS;
        int fromPos = toPos - duration + 1;
        for (int i = _latestPos + fromPos; i <= _latestPos + toPos; i++) {
            sum += _data[i % _duration];
        }
        return sum / duration;
    }
}

void Throughput::shift(int pos, long minTS) {
    if (pos >= _duration) {
        _latestPos = 0;
        _latestTS = minTS;
        for (int i = 0; i < _duration; i++) {
            _data[i] = 0;
        }
    } else {
        for (int i = _latestPos; i < _latestPos + pos; i++) {
            _data[i % _duration] = 0;
        }
        _latestPos = (_latestPos + pos) % _duration;
        _latestTS += pos;
    }
}

void Throughput::print() {
    std::cout << _latestTS << ": ";
    for (int i = _latestPos; i < _latestPos + _duration; i++) {
        std::cout << _data[i % _duration] << " ";
    }
    std::cout << std::endl;
}