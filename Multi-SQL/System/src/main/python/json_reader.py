import json
import numpy as np
import mongo
import onehot
import re
import pymongo

# 获取json文件的路径名
def get_item_list():
    dic = {'id': '',
           'authorid': '',
           'topic': '',
           'date': '',
           'blog': {
               'title': '',
               'key words': '',
               'content': ''
           }
           }
    res_list = []

    def genSq(d, sq):
        for i in d.keys():
            if isinstance(d[i], dict):
                genSq(d[i], sq + i + '.')
            else:
                res_list.append(sq + i)

    genSq(dic, '')
    return res_list


# 编码最频繁字段
def get_frequently_field(dic, field, seed):
    np.random.seed(seed)
    res = np.zeros([1, 30000])
    origin = []
    size = len(dic)
    random_list = np.random.choice(size, 10000, replace=False)
    for i in range(size):
        try:
            origin.append(get_key(dic, i, field))
        except KeyError:
            pass
    origin.sort()
    for i in range(10000):
        res[0][i] = random_list[i]+1
    return res, origin


def get_key(dic, k, field):
    res = dic[k]
    fields = field.split('.')
    for key in fields:
        res = res[key]
    return res


def load_blog_data(path):
    data_list = []
    #col = mongo.mongoConnect()
    with open(path, 'r') as f:
        for json_str in f.readlines():  # 按行读取json文件，每行为一个字符串
            data = json.loads(json_str)  # 将字符串转化为列表
            data_list.append(data)
            #col.insert(data)
    return data_list

'''
# 部分查询负载生成
def getpartNumQuery(res, origin_map, field, seed):
    np.random.seed(seed)
    size = len(origin_map)
    a = np.random.randint(0, size, 1)[0]
    b = np.random.randint(a, size-50, 1)[0]
    s = np.random.randint(a, b, 10000)
    t = np.random.randint(0, 50, 5000)
    for i in range(10000):
        res[0][i+10000] = s[i] + 1
    for i in range(5000):
        res[0][i+20000] = res[0][i+10000]+t[i]
    return res
'''

def get_workload(origin_map, seed):
    np.random.seed(seed)
    num_list = []
    size = len(origin_map)
    a = np.random.randint(0, size, 1)[0]
    b = np.random.randint(a, size - 10000, 1)[0]
    s = np.random.randint(a, b, 20010)
    t = np.random.randint(0, 10000, 10)
    for i in range(20010):
        num_list.append(origin_map[s[i]+1])
    for i in range(10):
        num_list.append(origin_map[s[i]+1+t[i]])
    sql_1 = 'Select document.id from document where document.date = '
    sql_2 = 'Select document.id from document where document.date between '
    with open('./new_workload', 'w') as f:
        for i in range(10):
            f.write(sql_2+"\""+str(num_list[i])+"\""+' and '+"\""+str(num_list[i+20010])+"\""+';\n')
        for i in range(20000):
            f.write(sql_1+"\""+str(num_list[i+10])+"\""+';\n')




def load_blog_workload(res, origin_map, path, doc_name, field):
    field = doc_name+'.'+field
    sum_num = []
    output = np.zeros([1, 4])
    with open(path, 'r') as f:
        pattern = re.compile(r'select .* where '+field+' (.*);', re.I)
        for sql_str in f.readlines():
            m = pattern.search(sql_str)
            sum_num.append(m.group(1))
    size = len(sum_num)
    random_list = np.random.choice(size, 10000, replace=False)
    for i in range(10000):
        
        num_str = sum_num[random_list[i]]
        a, b = parse_sql(num_str)
        #if a in origin_map:
        try:
            res[0][i+10000] = origin_map.index(a)+1
            if b == 0:
                res[0][i+20000] = 0
            else:
                res[0][i+20000] = origin_map.index(b)+1
        #else:
        except:
            res[0][i+10000] = 0
            res[0][i+20000] = 0
        
    return res


def parse_sql(string):

    if '=' in string:
        pattern = re.compile('= \"?(.*)\"?', re.I)
        m = pattern.match(string)
        return m.group(1), 0
    else:
        pattern = re.compile('between \"?(.*)\"? and \"?(.*)\"?', re.I)
        m = pattern.match(string)
        return m.group(1), m.group(2)



def get_in_output(n):
    data_list = load_blog_data('data/blog_dox.json')
    item_list = get_item_list()
    res, origin_map = get_frequently_field(data_list, item_list[3], 14)
    # get_workload(origin_map, 0)
    res = load_blog_workload(res, origin_map, 'workload/new_workload', 'document', item_list[3])
    col = mongo.mongoConnect()
    input = onehot.get_onehot(res)
    output = mongo.testout(col, res, origin_map, item_list[3])
    print(input.shape)
    print(output)
    for seed in range(1, n):
        item_list = get_item_list()
        res, origin_map = get_frequently_field(data_list, item_list[3], seed)
        res = load_blog_workload(res, origin_map, 'workload/new_workload', 'document', item_list[3])
        col = mongo.mongoConnect()
        input = np.concatenate((input, onehot.get_onehot(res)), axis=0)
        output = np.concatenate((output, mongo.testout(col, res, origin_map, item_list[3])), axis=0)
        print(input.shape)
        print(output)

    np.save("input.npy", input)
    np.save("output.npy", output)



if __name__ == '__main__':

    data_index_load('data/blog_dox.json')

