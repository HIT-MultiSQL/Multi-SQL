import sys
import json
import pymongo
import re
import numpy as np

def load_blog_workload_lite(path):
    sum_num = 0
    document = None
    fields = {}
    frequent = []
    eg = []
    res = []
    with open(path, 'r') as f:
        #pattern = re.compile(r'between', re.I)
        pattern = re.compile(r'where ([a-zA-Z0-9]*\.[a-zA-Z0-9]*) ', re.I)
        for sql_str in f.readlines():
            m = pattern.search(sql_str)
            if m:
                field = m.group(1)
                if field in fields.keys():
                    fields[field] += 1
                else:
                    fields[field] = 1
                sum_num += 1
                eg.append(sql_str)
        eg_num = min(5, len(eg))
        random_list = np.random.choice(len(eg), eg_num, replace=False)
        for i in range(eg_num):
            res.append(eg[random_list[i]])
        for x in fields.items():
            if x[1]/sum_num >= 0.3:
                frequent.append(x[0])

    return sum_num, frequent, res



def upload_worklord(path, name):
    sum_num, frequents, res = load_blog_workload_lite(path)

    details = 'Details:\n' \
              'path:'+path+'\n'\
              'This is a workload.\n' \
              'It contains ' + str(sum_num) + ' nodes.\n' \
              'The frequent field:\n'\

    fields = ''
    for field in frequents:
        details += field + ', '
        fields += field+','
    details = details[:-2]
    fields = fields[:-1]
    configurations = 'Configurations:\n' \
                     'name: ' + name + '\n' \
                     'eg:\n'
    for line in res:
        configurations += line

    return details, configurations, fields

if __name__ == '__main__':
    my_args = []
    for i in range(1, len(sys.argv)):  
        my_args.append(sys.argv[i])

    details, configurations, fields = upload_worklord(my_args[0],my_args[1])
    print(details)
    print("block")
    print(configurations)
    print("block")
    print(fields)

