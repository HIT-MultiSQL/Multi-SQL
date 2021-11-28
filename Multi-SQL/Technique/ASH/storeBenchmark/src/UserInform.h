//
// Created by yanhao on 2020/7/30.
//

#ifndef STOREBENCHMARK_USERINFORM_H
#define STOREBENCHMARK_USERINFORM_H

#include <string>
#include "dataDesc.h"

using namespace std;
class UserInform {
public:
    const int &getId() const;
    void setId(int id);
    const std::string &getName() const;
    void setName(const std::string &name);
    int getAge() const;
    void setAge(int age);
    char getGender() const;
    void setGender(char gender);
    const std::string &getOccupation() const;
    void setOccupation(const std::string &occupation);
    const std::string getRegister_date() const;
    void setRegister_date(const std::string register_date);
private:
    int _id = 0;
    std::string _name;
    int _age = 0;
    char _gender = '\0';
    std::string _occupation;
    std::string _register_date;
};

struct UserInformStruct {
    int id = 0;
    char* name = nullptr;
    uint8_t age = 0;
    char* gender = nullptr;
    char* occupation = nullptr;
    char* register_date = nullptr;
    ~UserInformStruct() {
        delete[] name;
        delete gender;
        delete[] occupation;
        delete[] register_date;
    }
};
#endif //STOREBENCHMARK_USERINFORM_H
