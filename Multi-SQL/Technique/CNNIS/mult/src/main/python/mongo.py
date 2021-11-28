import pymongo
import time
import numpy as np
import random


def testout(col, res, map, flied, fv):
    timelist = []
    partfilter = {
        flied: {
            "$gte": map[int(np.min(res[0][10000:20000])-1)],
            "$lte": map[int(np.max(res[0][10000:20000])-1)]
        }
    }
    col.drop_indexes()
    #result = random.randint(0, 6)
    result = 0
    if result == 0:
        timelist.append(testtime(col, res, map, flied))
    elif result == 1:
        col.create_index([(flied, pymongo.DESCENDING)], name=flied)
        timelist.append(testtime(col, res, map, flied))
        col.drop_index(flied)
    elif result == 2:
        col.create_index([(flied, pymongo.DESCENDING)], sparse=True, name=flied)
        timelist.append(testtime(col, res, map, flied))
        col.drop_index(flied)
    elif result == 3:
        col.create_index([(flied, pymongo.DESCENDING)], partialFilterExpression=partfilter, name=flied)
        timelist.append(testtime(col, res, map, flied))
        col.drop_index(flied)
    elif result == 4:
        col.create_index([(flied, pymongo.HASHED)], name=flied)
        timelist.append(testtime(col, res, map, flied))
        col.drop_index(flied)
    elif result == 5:
        col.create_index([(flied, pymongo.HASHED)], sparse=True, name=flied)
        timelist.append(testtime(col, res, map, flied))
        col.drop_index(flied)
    elif result == 6:
        col.create_index([(flied, pymongo.HASHED)], partialFilterExpression=partfilter, name=flied)
        timelist.append(testtime(col, res, map, flied))
        col.drop_index(flied)

    if fv[0] == 0:
        timelist.append(testtime(col, res, map, flied))
    elif fv[3] == 1:
        if fv[1] == 1:
            col.create_index([(flied, pymongo.DESCENDING)], sparse=True, name=flied)
            timelist.append(testtime(col, res, map, flied))
            col.drop_index(flied)
        elif fv[2] == 1:
            col.create_index([(flied, pymongo.DESCENDING)], partialFilterExpression=partfilter, name=flied)
            timelist.append(testtime(col, res, map, flied))
            col.drop_index(flied)
        else:
            col.create_index([(flied, pymongo.DESCENDING)], name=flied)
            timelist.append(testtime(col, res, map, flied))
            col.drop_index(flied)
    else:
        if fv[1] == 1:
            col.create_index([(flied, pymongo.HASHED)], sparse=True, name=flied)
            timelist.append(testtime(col, res, map, flied))
            col.drop_index(flied)
        elif fv[2] == 1:
            col.create_index([(flied, pymongo.HASHED)], partialFilterExpression=partfilter, name=flied)
            timelist.append(testtime(col, res, map, flied))
            col.drop_index(flied)
        else:
            col.create_index([(flied, pymongo.HASHED)], name=flied)
            timelist.append(testtime(col, res, map, flied))
            col.drop_index(flied)

    print(timelist)
    rate = (timelist[0]-timelist[1])/timelist[0] * 100

    return rate


def testtime(col, res, map, flied):
    res = res[0][10000:]
    begin = time.perf_counter()
    random_list = np.random.randint(0, 10000, 100)
    for i in random_list:
        # !!
        if res[i + 10000] != 0:
            partfilter = {
                flied: {
                    "$gte": map[int(res[i]-1)],
                    "$lte": map[int(res[i + 10000]-1)],
                }
            }

            djson = col.find(partfilter)
            for i in djson:
                i
        else:
            d = {}
            d[flied] = map[int(res[i]-1)]
            djson = col.find(d)
            # start = time.time()
            for i in djson:
                i
            # end = time.time()
            # print(end-start)
            pass
    end = time.perf_counter()
    totaltime = end - begin
    return totaltime



if __name__ == '__main__':
    res = np.loadtxt('res.txt')
    print(res[12000])

