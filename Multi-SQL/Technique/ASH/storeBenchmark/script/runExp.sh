#!/bin/bash
workdir=$(cd $(dirname $0); pwd)
cd ${workdir}/cmake-build-debug/
for i in `seq 24 24`
do
    time=$(date "+%Y-%m-%d %H:%M:%S")
    echo "${time} run test ${i}"
    ./main $i > ${workdir}/ctest/out2.log
done
