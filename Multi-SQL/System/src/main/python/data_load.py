import sys

import copy
import json
import pymongo
import re
import numpy as np
import json


def mongoConnect(database, collection):
    myclient = pymongo.MongoClient('mongodb://localhost:27017/')
    myclient.admin.authenticate('root', '123456')
    mydb = myclient[database]
    col = mydb[collection]
    return col


def load_blog_data(path, database, collection):
    data_list = []
    #col = mongoConnect(database, collection)
    count = 0
    with open(path, 'r') as f:
        for json_str in f.readlines():  # 按行读取json文件，每行为一个字符串
            data = json.loads(json_str)  # 将字符串转化为列表
            data_list.append(data)
            #col.insert_one(copy.copy(data))
            count += 1
    return data_list, count, database, collection


def upload_data(path, name, database, collection):
    data_list, num, db, collection = load_blog_data(path, database, collection)
    details = 'Details:\n' \
              'path:'+path+'\n'\
              'This is a doc data.\n' \
              'It contains ' + str(num) + ' nodes.'
    configurations = 'Configurations:\n' \
                     'name: ' + name + '\n' \
                    'eg:\n'+str(json.dumps(data_list[0], indent=4, ensure_ascii=False))

    storage = 'The destination database is ' + db + '.' + collection

    return details, configurations, storage




if __name__ == '__main__':
    my_args = []
    for i in range(1, len(sys.argv)):  
        my_args.append(sys.argv[i])
    detials, configurations, storage = upload_data(my_args[0],my_args[1],my_args[2],my_args[3])
    print(detials)
    print("block")
    print(configurations)
    print("block")
    print(storage)