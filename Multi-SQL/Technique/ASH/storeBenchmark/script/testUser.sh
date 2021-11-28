#!/bin/bash
workdir=$(cd $(dirname $0); pwd)
cd ${workdir}/cmake-build-debug/
time=$(date "+%Y-%m-%d %H:%M:%S")
echo "${time} run test W1T1"
./main 0 10000 0 100000 1 > ${workdir}/W1T1.log