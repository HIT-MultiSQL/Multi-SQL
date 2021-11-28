//
// Created by yanhao on 2020/7/30.
//
#include "UserInform.h"

const int &UserInform::getId() const {
    return _id;
}

void UserInform::setId(int id) {
    _id = id;
}

const std::string &UserInform::getName() const {
    return _name;
}

void UserInform::setName(const std::string &name) {
    _name = name;
}

int UserInform::getAge() const {
    return _age;
}

void UserInform::setAge(int age) {
    _age = age;
}

char UserInform::getGender() const {
    return _gender;
}

void UserInform::setGender(char gender) {
    _gender = gender;
}

const std::string &UserInform::getOccupation() const {
    return _occupation;
}

void UserInform::setOccupation(const std::string &occupation) {
    _occupation = occupation;
}

const std::string UserInform::getRegister_date() const {
    return _register_date;
}

void UserInform::setRegister_date(const std::string register_date) {
    _register_date = register_date;
}

