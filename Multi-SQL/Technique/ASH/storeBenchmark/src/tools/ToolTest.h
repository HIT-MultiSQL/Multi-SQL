//
// Created by iron on 2019/12/17.
//

#ifndef STOREBENCHMARK_TEST_H
#define STOREBENCHMARK_TEST_H

#include <iostream>
#include <vector>
#include "Histogram.h"

using namespace std;
void histogramTest() {
    Histogram<int> h(1000, 10);
    int seq = 1;
    for(int i = 0; i < 10; i++) {
        for (int j = 0; j < 1000; j++) {
            h.offerSample(seq);
            seq++;
        }
        vector<int> bounds = h.getBounds();
        cout << h.getSpanOffset(1) << " ";
        for (int j = 0; j < bounds.size(); j++) {
            cout << bounds[j] << " ";
            int test = bounds[j] + 1;
            cout << h.getSpanOffset(test) << " ";
        }
        cout << endl;
    }
}

#endif //STOREBENCHMARK_TEST_H
