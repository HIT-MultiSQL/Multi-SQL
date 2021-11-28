import ctypes
import inspect
import os
import random
import time

import pymysql


def count_rate(p_list):
    p_dict = dict()
    for p in p_list:
        if p not in p_dict:
            p_dict[p] = 1.0
        else:
            p_dict[p] += 1.0
    for p in p_dict.keys():
        p_dict[p] = p_dict[p] / len(p_list)
    return p_dict


def async_raise(tid, exctype):
    """raises the exception, performs cleanup if needed"""
    tid = ctypes.c_long(tid)
    if not inspect.isclass(exctype):
        exctype = type(exctype)
    res = ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, ctypes.py_object(exctype))
    if res == 0:
        raise ValueError("invalid thread id")
    elif res != 1:
        # """if it returns a number greater than one, you're in trouble,
        # and you should call it again with exc=NULL to revert the effect"""
        ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, None)
        raise SystemError("PyThreadState_SetAsyncExc failed")


def stop_thread(thread):
    async_raise(thread.ident, SystemExit)


def transfer_neo4j_import(p_list, flag=1):
    neo4j_home = "/home/sde/BigData_zhanghaoran/run/neo4j-community-3.5.14/"
    start = time.perf_counter()
    db = pymysql.connect(host='localhost', port=3306, user='root',
                         passwd='123456', db='watdiv30', charset='utf8')
    cursor = db.cursor()
    f_s = open(neo4j_home + '/import/s.csv', 'w', encoding='utf8')
    f_p = open(neo4j_home + '/import/p.csv', 'w', encoding='utf8')
    f_o = open(neo4j_home + '/import/o.csv', 'w', encoding='utf8')
    f_s.write("subjectId:ID\tname\t:LABEL\n")
    f_o.write("objectId:ID\tname\t:LABEL\n")
    f_p.write(":START_ID\t:END_ID\t:TYPE\n")
    print("transferring p list:" + str(p_list))
    sql = ""
    if flag == 0:
        sql = "SELECT * FROM yagoData WHERE pname in %s"
    elif flag == 1 or flag == 4 or flag == 5 or flag == 6:
        sql = "SELECT * FROM watdiv WHERE pname in %s"
    elif flag == 2:
        sql = "SELECT * FROM bio2rdf WHERE pname in %s"
    elif flag == 3:
        sql = "SELECT * FROM lubm WHERE pname in %s"
    try:
        cursor.execute(sql, (p_list,))
        results = cursor.fetchall()
    except:
        db.ping()
        cursor = db.cursor()
        cursor.execute(sql, (p_list,))
        results = cursor.fetchall()
    f_sw = set()
    f_ow = set()
    print("transfer result length:" + str(len(results)))
    for result in results:
        s_id = str(hash(result[0]))
        o_id = str(hash(result[2]))
        if s_id not in f_sw:
            f_s.write(s_id + '\t' + result[0] + '\tsubject\n')
            f_sw.add(s_id)
        if o_id not in f_ow:
            f_o.write(o_id + '\t' + result[2].rstrip('\r') + '\tobject\n')
            f_ow.add(o_id)
        f_p.write(s_id + "\t" + o_id + "\t" + result[1] + "\n")
    f_s.close()
    f_p.close()
    f_o.close()
    db.close()
    os.system(
        "rm -rf '" + neo4j_home + "/data/databases/graph2.db' > log.txt")
    os.system("'" + neo4j_home + "/bin/neo4j' restart > log.txt")
    os.system("'" + neo4j_home + "/bin/neo4j' stop > /mnt/hgfs/research_data/new/log.txt")
    os.system("'" + neo4j_home + "/bin/neo4j-import' "
              "--into '" + neo4j_home + "/data/databases/graph2.db' "
              "--ignore-empty-strings=true "
              "--skip-bad-entries-logging=true "
              "--read-buffer-size=128m "
              "--nodes '" + neo4j_home + "/import/s.csv' "
              "--nodes '" + neo4j_home + "/import/o.csv' "
              " --relationships '" + neo4j_home + "/import/p.csv'  "
              "--delimiter='\t' "
              "--skip-bad-relationships "
              "--skip-duplicate-nodes=true "
              "--bad-tolerance=50000000 "
              " > log.txt")
    # " > /mnt/hgfs/research_data/new/transfer_result.txt")
    os.system("'" + neo4j_home + "/bin/neo4j' start  > log.txt")
    time.sleep(60)
    end = time.perf_counter()
    return end - start


def get_random_number(random_file_name, query_length):
    try:
        shuffle_numbers = []
        with open(random_file_name, 'r', encoding='utf8') as random_f:
            random_lines = random_f.readlines()
            for line in random_lines:
                shuffle_numbers.append(int(line.rstrip('\r\n')))
    except FileNotFoundError:
        shuffle_numbers = list(range(0, query_length))
        random.shuffle(shuffle_numbers)
        with open(random_file_name, 'w', encoding='utf8') as f:
            for number in shuffle_numbers:
                f.write(str(number) + "\r\n")
    return shuffle_numbers


def get_query_order(random_file_name, query_length, flag=0):
    """
    获取query进行的顺序
    :param random_file_name: 存储乱序顺序的文件路径
    :param query_length: query的数量
    :param flag: 0为调参模式，只有10个固定query
                 1为顺序模式，20个query按顺序排列
                 2为乱序模式，20个query乱序排列，且每个数据集的query的排列都不一样，
                    但同时每个数据集的乱序每次都一样
    :return: query进行的顺序
    """
    if flag == 0:
        return [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]
    elif flag == 1:
        return list(range(0, query_length))
    elif flag == 2:
        return get_random_number(random_file_name, query_length)
    else:
        return None
