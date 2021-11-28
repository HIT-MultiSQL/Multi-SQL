//
// Created by iron on 2019/12/11.
//

#ifndef STOREBENCHMARK_HISTOGRAM_H
#define STOREBENCHMARK_HISTOGRAM_H

#include <iostream>
#include <algorithm>
#include <vector>

template<class T>
class Histogram{
public:
    /**
     * 等高直方图，使用水库抽样的方法，将样本分为大小相同的spanNum个区间，计算每个区间的起点和终点
     * @param sampleSize
     * @param spanNum
     */
    Histogram(int sampleSize, int spanNum);
    int getSpanOffset(const T& data);
    void offerSample(const T& data);
    std::vector<T> getBounds();
    int getSpanNum();
private:
    void reCalculate();
    int _sampleSize;
    int _spanNum;
    long _seqNum = 0;
    bool _update = false;
    std::vector<T> _bounds;
    std::vector<T> _samples;
};

template<class T>
Histogram<T>::Histogram(int sampleSize, int spanNum):_sampleSize(sampleSize), _spanNum(spanNum) {
    if (sampleSize < spanNum || spanNum < 2) {
        std::cout << "invalid Histogram parameters" << std::endl;
        return;
    }
    _samples.resize(sampleSize);
    _bounds.resize(spanNum - 1);
}

template<class T>
int Histogram<T>::getSpanOffset(const T &data) {
    if (!_seqNum) return 0;
    if (_update) {
        reCalculate();
    }
    typename std::vector<T>::iterator low;
    low = std::upper_bound(_bounds.begin(), _bounds.end(), data);
    return low - _bounds.begin();
}

template<class T>
void Histogram<T>::offerSample(const T &data) {
    if (_seqNum < _sampleSize) {
        _samples[_seqNum] = data;
        _seqNum++;
        _update = true;
    } else {
        _seqNum++;
        int rand = random() % _seqNum;
        if (rand < _sampleSize) {
            _samples[rand] = data;
            _update = true;
        }
    }
}

template<class T>
std::vector<T> Histogram<T>::getBounds() {
    if (_update) {
        reCalculate();
    }
    return _bounds;
}

template<class T>
void Histogram<T>::reCalculate() {
    sort(_samples.begin(), _samples.end());
    for (long i = 0; i < _spanNum - 1; i ++) {
        long pos = _sampleSize * (i + 1) / _spanNum;
        _bounds[i] = _samples[pos];
    }
    _update = false;
}

template<class T>
int Histogram<T>::getSpanNum() {
    return _spanNum;
}



#endif //STOREBENCHMARK_HISTOGRAM_H
