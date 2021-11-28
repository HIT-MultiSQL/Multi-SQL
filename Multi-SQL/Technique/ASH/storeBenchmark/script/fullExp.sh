#!/bin/bash
workdir=$(cd $(dirname $0); pwd)
cd ${workdir}/../cmake-build-debug/
time=$(date "+%Y-%m-%d %H:%M:%S")
echo "${time} run test W1T1"
./main 0 9000000 10000 10000 0 0 3 > ${workdir}/../../W1T1.log
time=$(date "+%Y-%m-%d %H:%M:%S")
echo "${time} run test W1T2"
./main 0 9000000 10000 10000 0 0 1 > ${workdir}/../../W1T2.log
time=$(date "+%Y-%m-%d %H:%M:%S")
echo "${time} run test W2T1"
./main 9000000 3000000 30000 30000 5 5 3 > ${workdir}/../../W2T1.log
time=$(date "+%Y-%m-%d %H:%M:%S")
echo "${time} run test W3T1"
./main 12000000 0 5000 5000 15 15 3 > ${workdir}/../../W3T1.log
time=$(date "+%Y-%m-%d %H:%M:%S")
echo "${time} run test W3T2"
./main 12000000 0 5000 5000 15 15 4 > ${workdir}/../../W3T2.log
time=$(date "+%Y-%m-%d %H:%M:%S")
echo "${time} run test W4T1"
./main 12000000 0 0 0 30 30 3 > ${workdir}/../../W4T1.log
time=$(date "+%Y-%m-%d %H:%M:%S")
echo "${time} run test W4T2"
./main 12000000 0 0 0 30 30 4 > ${workdir}/../../W4T2.log
time=$(date "+%Y-%m-%d %H:%M:%S")
echo "${time} run test W4T3"
./main 12000000 0 0 0 30 30 2 > ${workdir}/../../W4T3.log
