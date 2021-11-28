//
// Created by iron on 2019/12/19.
//

#ifndef STOREBENCHMARK_FIXSIZESAMPLE_H
#define STOREBENCHMARK_FIXSIZESAMPLE_H

#include <boost/dynamic_bitset.hpp>

template <class T>
class FixSizeSample {
public:
    explicit FixSizeSample(unsigned int size);
    void offerSample(const T& data);
    std::vector<T>& getSamples();
    void fillBitSet(Histogram<T>& distribution, boost::dynamic_bitset<>& bitset);
private:
    std::vector<T> _samples;
    int _seqNum = 0;
    unsigned int _sampleSize;
};

template<class T>
void FixSizeSample<T>::offerSample(const T &data) {
    if (_seqNum < _sampleSize) {
        _samples.push_back(data);
        _seqNum++;
    } else {
        _seqNum++;
        int rand = random() % _seqNum;
        if (rand < _sampleSize) {
            _samples[rand] = data;
        }
    }
}

template<class T>
FixSizeSample<T>::FixSizeSample(unsigned int size) : _sampleSize(size) {
    _samples.reserve(size);
}

template<class T>
vector<T> &FixSizeSample<T>::getSamples() {
    return _samples;
}

template<class T>
void FixSizeSample<T>::fillBitSet(Histogram<T> &distribution, boost::dynamic_bitset<> &bitset) {
    for (int i = 0; i < _samples.size(); i++) {
        bitset[distribution.getSpanOffset(_samples[i])] = 1;
    }
}


#endif //STOREBENCHMARK_FIXSIZESAMPLE_H
