//
// Created by ironwei on 2020/2/22.
//

#ifndef STOREBENCHMARK_VALUESOURCE_H
#define STOREBENCHMARK_VALUESOURCE_H

#include <string>
#include <vector>
#include "wiredtiger.h"
#include "wiredtiger_ext.h"

class ValueSource {
public:
    explicit ValueSource(unsigned int valueLength);
    virtual std::string getWTValueFormat() const = 0;
    std::string getNextValueString(int pos);
    virtual void wtSetNextValue(WT_CURSOR* cursor, int pos) = 0;
    virtual int getValueFieldsNum() const = 0;
    void setRandomPara(unsigned int valueRange, unsigned int consecutive);
    virtual ~ValueSource() = default;
    void initSplitMethod();
    void initRandomPara();
    void generateValues();
protected:
    std::vector<int> _valueLength;
    std::vector<int> _valueRange;
    std::vector<int> _consecutive;
    std::vector<std::vector<std::string> > _values;
};

class Value1Source : public ValueSource {
public:
    explicit Value1Source(unsigned int valueLength) : ValueSource(valueLength) {};
    std::string getWTValueFormat() const override {return std::string("S");};
    void wtSetNextValue(WT_CURSOR* cursor, int pos) override;
    int getValueFieldsNum() const override { return 1; };
    ~Value1Source() override = default;
};

class Value2Source : public ValueSource {
public:
    explicit Value2Source(unsigned int valueLength) : ValueSource(valueLength) {
        initSplitMethod();
        initRandomPara();
    };
    std::string getWTValueFormat() const override {return std::string("SS");};
    void wtSetNextValue(WT_CURSOR* cursor, int pos) override;
    int getValueFieldsNum() const override { return 2; };
    ~Value2Source() override = default;
};

class Value3Source : public ValueSource {
public:
    explicit Value3Source(unsigned int valueLength) : ValueSource(valueLength) {
        initSplitMethod();
        initRandomPara();
    };
    std::string getWTValueFormat() const override {return std::string("SSS");};
    void wtSetNextValue(WT_CURSOR* cursor, int pos) override;
    int getValueFieldsNum() const override { return 3; };
    ~Value3Source() override = default;
};

class Value4Source : public ValueSource {
public:
    explicit Value4Source(unsigned int valueLength) : ValueSource(valueLength) {
        initSplitMethod();
        initRandomPara();
    };
    std::string getWTValueFormat() const override {return std::string("SSSS");};
    void wtSetNextValue(WT_CURSOR* cursor, int pos) override;
    int getValueFieldsNum() const override { return 4; };
    ~Value4Source() override = default;
};

class Value6Source : public ValueSource {
public:
    explicit Value6Source(unsigned int valueLength) : ValueSource(valueLength) {
        initSplitMethod();
        initRandomPara();
    };
    std::string getWTValueFormat() const override {return std::string("SSSSSS");};
    void wtSetNextValue(WT_CURSOR* cursor, int pos) override;
    int getValueFieldsNum() const override { return 6; };
    ~Value6Source() override = default;
};

class Value8Source : public ValueSource {
public:
    explicit Value8Source(unsigned int valueLength) : ValueSource(valueLength) {
        initSplitMethod();
        initRandomPara();
    };
    std::string getWTValueFormat() const override {return std::string("SSSSSSSS");};
    void wtSetNextValue(WT_CURSOR* cursor, int pos) override;
    int getValueFieldsNum() const override { return 8; };
    ~Value8Source() override = default;
};

class Value10Source : public ValueSource {
public:
    explicit Value10Source(unsigned int valueLength) : ValueSource(valueLength) {
        initSplitMethod();
        initRandomPara();
    };
    std::string getWTValueFormat() const override {return std::string("SSSSSSSSSS");};
    void wtSetNextValue(WT_CURSOR* cursor, int pos) override;
    int getValueFieldsNum() const override { return 10; };
    ~Value10Source() override = default;
};

class Value12Source : public ValueSource {
public:
    explicit Value12Source(unsigned int valueLength) : ValueSource(valueLength) {
        initSplitMethod();
        initRandomPara();
    };
    std::string getWTValueFormat() const override {return std::string("SSSSSSSSSSSS");};
    void wtSetNextValue(WT_CURSOR* cursor, int pos) override;
    int getValueFieldsNum() const override { return 12; };
    ~Value12Source() override = default;
};

class Value16Source : public ValueSource {
public:
    explicit Value16Source(unsigned int valueLength) : ValueSource(valueLength) {
        initSplitMethod();
        initRandomPara();
    };
    std::string getWTValueFormat() const override {return std::string("SSSSSSSSSSSSSSSS");};
    void wtSetNextValue(WT_CURSOR* cursor, int pos) override;
    int getValueFieldsNum() const override { return 16; };
    ~Value16Source() override = default;
};

class Value20Source : public ValueSource {
public:
    explicit Value20Source(unsigned int valueLength) : ValueSource(valueLength) {
        initSplitMethod();
        initRandomPara();
    };
    std::string getWTValueFormat() const override {return std::string("SSSSSSSSSSSSSSSSSSSS");};
    void wtSetNextValue(WT_CURSOR* cursor, int pos) override;
    int getValueFieldsNum() const override { return 20; };
    ~Value20Source() override = default;
};
#endif //STOREBENCHMARK_VALUESOURCE_H
