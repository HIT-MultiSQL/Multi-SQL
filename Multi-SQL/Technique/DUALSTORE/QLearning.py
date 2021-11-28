import os
import re
import sys
import threading
import pymysql
import random
import time
from py2neo import Graph
import configparser
from py2neo.packages.httpstream import http
from FindSubQuery import find_sub_query
from util import stop_thread, count_rate, transfer_neo4j_import, get_query_order
http.socket_timeout = 9999


class QLearning:
    def __init__(self, config):
        print("connecting...")
        self.cf = configparser.ConfigParser()
        self.cf.read(config, encoding="utf-8")
        self.host = self.cf.get("total", "host")
        self.port = int(self.cf.get("total", "port"))
        self.user = self.cf.get("total", "user")
        self.password = self.cf.get("total", "password")
        self.db = self.cf.get("total", "db")
        self.charset = self.cf.get("total", "charset")
        self.graph = Graph(self.cf.get("rdb-gdb", "graph_uri"), username='neo4j', password='123456', secure=False,
                           bolt=False)  # neo4j连接
        self.gamma = float(self.cf.get("rdb-gdb", "gamma"))  # gamma，折旧率
        self.alpha = float(self.cf.get("rdb-gdb", "alpha"))  # alpha，学习率
        self.transfer_percent = float(self.cf.get("rdb-gdb", "threshold"))  # neo4j / mysql threshold
        self.prob = float(self.cf.get("rdb-gdb", "prob"))  # 随机概率
        self.neo4j_times = float(self.cf.get("rdb-gdb", "neo4j_times"))  # mysql超时时长倍数
        self.max_time = float(self.cf.get("rdb-gdb", "max_time"))  # 超时时长
        f_p = open(self.cf.get("total", "p_record_dir") + self.cf.get("total", "db") + "_p_record.txt"
                   , 'r', encoding='utf8')  # 存储了p和p的个数文件
        lines = f_p.readlines()
        f_p.close()
        self.reward = dict()  # Reward矩阵
        self.q = dict()  # Q矩阵
        self.transfer_record = dict()  # 记录p是否transfer
        self.total_neo4j_number = 0  # Neo4j total number of transfered triples
        self.query_order = int(self.cf.get("rdb-gdb", "query_order"))
        self.numbers = dict()  # record the number of triples of predicate
        for line in lines:
            result = re.search("([^\t]*)\t([^\t]*)", line)
            self.numbers[result.group(1)] = result.group(2)
            self.reward[result.group(1)] = [[0, 0], [0, 0]]
            self.q[result.group(1)] = [[0, 0], [0, 0]]
            self.transfer_record[result.group(1)] = False
        self.total_number = int(self.cf.get("total", "triple_length"))

        self.p_count = dict()
        self.p_count_rev = dict()

        with open("p_count.txt", "r", encoding="utf8") as f:
            line = f.readline()
            while line:
                result = re.search("([^\t]*)\t([^\t]*)", line)
                p = result.group(1)
                num = result.group(2).rstrip("\n").replace(" ", "")
                if result:
                    self.p_count[p] = num
                    self.p_count_rev[num] = p
                line = f.readline()
        self.p_sum = dict()
        with open("p_sum.txt", "r", encoding="utf8") as f:
            line = f.readline()
            while line:
                result = re.search("([^\t]*)\t([^\t]*)\t([^\t]*)", line)
                file_num = result.group(1)
                dic = result.group(3).replace(")", "").replace("(", "").replace("[", "").replace("]", "")\
                    .replace(" ", "").split(",")
                for i in range(0, len(dic), 2):
                    self.p_sum[self.p_count_rev[dic[i]]] = file_num
                line = f.readline()

        self.neo4j_query_time = 0  # 记录Neo4j查询总时间，没有计入超时的时间
        self.second_query_time = 0  # 每次第二个查询记录时间
        self.total_batch_time = 0  # 所有Batch Online总时间
        self.this_time_neo4j_results = []  # 获取本次Neo4j查询结果
        self.this_time_sql = 0.0  # 记录本次MySQL查询时间
        self.this_time_neo = 0.0  # 记录本次Neo4j查询时间
        self.batch_time_list = []  # 保存每个Batch的Online时间
        print("Connecting success!")

    def rdb_query_time(self, query):
        """计算rdb的query时间"""
        db = pymysql.connect(host=self.host, port=self.port, user=self.user,
                             passwd=self.password, db=self.db, charset=self.charset)
        cursor = db.cursor()
        start = time.perf_counter()
        try:
            cursor.execute(query)
        except Exception as e:
            print("_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+")
            print(query)
            print(e)
            print("_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+")
        end = time.perf_counter()
        result = cursor.fetchall()
        print("MySQL query result length:" + str(len(result)))
        db.close()
        self.this_time_sql = end - start

    def neo_query_time(self, query, length):
        graph = Graph(self.cf.get("rdb-gdb", "graph_uri"), username='neo4j', password='123456', secure=False,
                      bolt=False)  # neo4j连接
        start = time.perf_counter()
        results = graph.run(query)
        end = time.perf_counter()
        result_list = []
        count = 0
        for result in results:
            result_list.append(tuple(result[:length]))
            count += 1
        print("Neo4j query result:" + str(len(list(set(result_list)))) + "\t")
        if count == 0:
            print(query)
        self.this_time_neo4j_results = list(set(result_list))
        self.this_time_neo = end - start

    def update_q(self, row, column, p):
        # 更新了Q10
        self.q[p][row][column] = (1.0 - self.alpha) * self.reward[p][row][column] + \
                          self.alpha * self.gamma * max(self.q[p][abs(row-column)][0], self.q[p][abs(row-column)][1])

    def update_after_transfer(self, mysql, neo4j_time, sub_p_rate, last_record):
        print("neo4j  query time: " + str(neo4j_time))
        t = threading.Thread(target=self.rdb_query_time, args=(mysql,))
        t.start()
        try:
            t.join(int(float(neo4j_time) * float(self.neo4j_times)))
        except RuntimeError:
            print(self.neo4j_times)
            print(neo4j_time)
            exit(0)
        print("mysql is still running:" + str(t.is_alive()))
        if t.is_alive():
            stop_thread(t)
            mysql_time = neo4j_time * self.neo4j_times
            print("stop mysql immadietely, time:" + str(mysql_time))
        else:
            mysql_time = self.this_time_sql
            print("mysql query successfully, time:" + str(mysql_time))
        improvement = mysql_time - neo4j_time
        for p in set(sub_p_rate.keys()):
            if p in last_record:
                self.reward[p][1][0] += improvement * sub_p_rate[p]
                self.update_q(1, 0, p)
            else:
                self.reward[p][0][1] += improvement * sub_p_rate[p]
                self.update_q(0, 1, p)
        self.this_time_sql = 0

    def rdb_second_query(self, query, parameters):
        if query == "":
            print("Because of the whole query is a sub query, there is no other query")
            return
        db = pymysql.connect(host=self.host, port=self.port, user=self.user,
                             passwd=self.password, db=self.db, charset=self.charset)
        cursor = db.cursor()
        start = time.perf_counter()
        query += ";"
        all_result = []
        show_flag = False
        for parameter in parameters:
            final_query = query.format(list(parameter))
            try:
                final_query = final_query.replace(";", "") + ";"
                if not show_flag:
                    print("Second Other Query Final Query: " + final_query)
                    show_flag = True
                cursor.execute(final_query)
                result = cursor.fetchall()
                if len(result) != 0:
                    all_result.append(result)
            except Exception as e:
                print("_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_++_+_+_+_+_")
                print(e)
                print("second " + query)
                print(parameter)
                print("++_)+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+")
                break

        end = time.perf_counter()
        self.second_query_time = end - start
        # for result_number in range(0, len(all_result), int(len(all_result) / 10)):
        #     print(all_result[result_number])
        print("The final query result length: " + str(len(all_result)))
        db.close()

    def show_transferred_p(self):
        p_list = []
        for p in self.transfer_record.keys():
            if self.transfer_record[p]:
                p_list.append(p)
        print("transferred p: " + str(p_list))

    def _q_learning_init(self):
        file_name = self.cf.get("total", "sql_query_dir") + self.db + "_sql_query.txt"
        file_cypher_name = self.cf.get("total", "cypher_query_dir") + self.db + "_cypher_query.txt"
        random_file_name = self.cf.get("total", "random_query_dir") + self.db + "_random.txt"
        with open(file_name, 'r', encoding='utf8') as f_sql:
            mysql_queries = f_sql.readlines()
        with open(file_cypher_name, 'r', encoding='utf8') as f_cypher:
            cypher_queries = f_cypher.readlines()
        query_order = get_query_order(random_file_name, len(mysql_queries), self.query_order)
        return mysql_queries, cypher_queries, query_order

    def q_learning(self):
        mysql_queries, cypher_queries, query_order = self._q_learning_init()
        batch_number = int(self.cf.get("rdb-gdb", "batch_number"))
        one_batch_length = len(query_order) / batch_number
        total_start = time.perf_counter()  # start: Total Time
        for batch_number in range(batch_number):
            start_batch_number = int(batch_number * one_batch_length)
            end_batch_number = int((batch_number + 1) * one_batch_length)
            if batch_number == 9:
                end_batch_number = len(mysql_queries) - 1
            batch_p_list = set()  # 存储每批次的p
            batch_start = time.perf_counter()  # start: Batch Time
            for query_number in query_order[start_batch_number:end_batch_number]:
                print("===================================================")
                print("start query online:" + str(query_number))
                # 首先判断子结构
                flag, sub_p_list, new_mysql, new_cypher, other_mysql, length = \
                    find_sub_query(mysql_queries[query_number], cypher_queries[query_number])
                p_exist_flag = True
                if flag:
                    for p in sub_p_list:
                        if p not in self.transfer_record:
                            p_exist_flag = False
                # 如果没有子结构
                if not flag or not p_exist_flag:
                    print("There is no sub query struct, start MySQL Query instead")
                    t = threading.Thread(target=self.rdb_query_time, args=(mysql_queries[query_number],))
                    t.start()
                    t.join(self.max_time)
                    if t.is_alive():
                        stop_thread(t)
                        mysql_time = self.max_time
                        print("Stop the mysql query immediately, time:" + str(mysql_time))
                    else:
                        mysql_time = self.this_time_sql
                        print("mysql query successfully , time:" + str(mysql_time))
                    print("immediately mysql query over, start next query")
                    continue
                else:
                    print("detected the sub query struct")
                print("Sub sql query: " + new_mysql)
                print("Sub cypher query: " + new_cypher)
                print("Other query: " + other_mysql)
                print("Length: " + str(length))

                # 获取p的占比
                sub_p_rate = count_rate(sub_p_list)
                print("Get the p rates: " + str(sub_p_rate))
                for p in sub_p_list:  # 取出本次子结构没有transfer的p
                    if not self.transfer_record[p]:
                        batch_p_list.add(p)

                # 判断是否全部在Neo4j中
                transfer_flag = False
                for p in sub_p_list:
                    if not self.transfer_record[p]:  # 有一个没transfer的就为True
                        transfer_flag = True
                self.show_transferred_p()
                # 如果一个没transfer的都没有,全部已经transfer
                if not transfer_flag:  # 如果一个没transfer的都没有,全部已经transfer
                    print("All p in sub query is transferred, use Neo4j to query")
                    t2 = threading.Thread(target=self.neo_query_time, args=(new_cypher, length))
                    t2.start()
                    t2.join(100)
                    if t2.is_alive():
                        stop_thread(t2)
                        print("Neo4j over time!")
                        continue
                    else:
                        print("Neo4j query success")

                    neo4j_time, result_list = self.this_time_neo, self.this_time_neo4j_results
                    # self.update_after_transfer(new_mysql, neo4j_time, sub_p_rate)
                    if len(result_list) != 0:
                        t1 = threading.Thread(target=self.rdb_second_query, args=(other_mysql, result_list))
                        t1.start()
                        t1.join(self.max_time)
                        if t1.is_alive():
                            stop_thread(t1)
                            print("Final mysql query overtime:" + str(self.max_time))
                        else:
                            print("Final query run successfully:" + str(self.second_query_time))
                    else:
                        print("Because of the sub query result length is 0, so jump over the other query")
                    continue
                # 如果有一个没transfer的，就在mysql中直接进行
                else:
                    print("Have some p not in neo4j, choose to use mysql")
                    t = threading.Thread(target=self.rdb_query_time, args=(mysql_queries[query_number],))
                    t.start()
                    t.join(self.max_time)
                    print("Mysql is still running:" + str(t.is_alive()))
                    if t.is_alive():
                        stop_thread(t)
                        print("Force stop mysql, time:" + str(self.max_time))
                    else:
                        print("Mysql query successfully, time:" + str(self.this_time_sql))
                    continue
            batch_end = time.perf_counter()  # end: Batch Time
            self.total_batch_time += batch_end - batch_start
            self.batch_time_list.append(batch_end - batch_start)
            print("The No." + str(batch_number) + " batch query Online over, time :" + str(batch_end - batch_start))
            # ==========================================================================================================
            # 结束一个批次的处理，开始Offline过程
            print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            print("Start process No." + str(batch_number) + "batch transferring")
            for query_number in query_order[start_batch_number:end_batch_number]:
                print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                print("Start process number: " + str(query_number))
                # 首先判断子结构
                flag, sub_p_list, new_mysql, new_cypher, other_mysql, length = \
                    find_sub_query(mysql_queries[query_number], cypher_queries[query_number])

                p_exist_flag = True
                if flag:
                    for p in sub_p_list:
                        if p not in self.transfer_record:
                            p_exist_flag = False
                # 如果没有子结构
                if not flag or not p_exist_flag:
                    continue
                last_time_record = []
                for p in self.transfer_record.keys():
                    if self.transfer_record[p]:
                        last_time_record.append(p)
                print("New SQL:" + new_mysql)
                print("New Cypher:" + new_cypher)
                print("Other SQL:" + other_mysql)
                # 判断子查询谓语是否包含在图数据库中
                transfer_flag = False
                for p in sub_p_list:
                    if not self.transfer_record[p]:  # 有一个没transfer的就为True
                        transfer_flag = True
                sub_p_rate = count_rate(sub_p_list)
                print("Get the p rate of the sub query" + str(sub_p_rate))
                if not transfer_flag:  # 如果一个没transfer的都没有,全部已经transfer
                    print("All p in sub query are in Neo4j")
                    t2 = threading.Thread(target=self.neo_query_time, args=(new_cypher, length))
                    t2.start()
                    t2.join(100)
                    if t2.is_alive():
                        stop_thread(t2)
                        print("Neo4j over time!" + str(self.max_time))
                        continue
                    else:
                        print("Neo4j query success")
                    neo4j_time, result_list = self.this_time_neo, self.this_time_neo4j_results

                    self.update_after_transfer(new_mysql, neo4j_time, sub_p_rate, last_time_record)
                    continue

                # 根据Q学习判断是否Transfer
                need_transfer = set()
                # 根据Q学习判断是否迁移
                total_reward = 0
                for p in sub_p_list:
                    total_reward += self.reward[p][0][1]
                print("Total reward: " + str(total_reward))
                if total_reward > 0:
                    print("Total reward > 0")
                    for p in sub_p_list:
                        need_transfer.add(p)
                elif total_reward == 0:
                    if random.randint(1, 101) < 100 * self.prob:  # 如果等于0，随机判断
                        print("Total reward = 0, random choose to transfer")
                        for p in sub_p_list:
                            need_transfer.add(p)
                    else:
                        print("Total reward = 0, random choose to continue")
                else:
                    print("Total reward < 0, choose to continue")

                if len(need_transfer) == 0:
                    print("No p to transfer, continue")
                    continue

                sub_total_number = 0
                for p in set(need_transfer):
                    if not self.transfer_record[p]:
                        sub_total_number += int(self.numbers[p])
                if self.total_neo4j_number + sub_total_number >= self.total_number * self.transfer_percent:
                    print("Because of the data in Neo4j is too much, now trying to delete some p")
                    for p in set(self.transfer_record.keys()):
                        if self.transfer_record[p]:
                            if self.reward[p][1][0] < 0:
                                print(p + " R10 < 0, be retrieved")
                                p_number = int(self.numbers[p])
                                self.transfer_record[p] = False
                                self.total_neo4j_number -= p_number
                            if self.reward[p][1][0] == 0:
                                if random.randint(1, 11) > 10 * self.prob:
                                    print(p + " R10=0, random choose to be retrieved")
                                    p_number = int(self.numbers[p])
                                    self.total_neo4j_number -= p_number
                                    self.transfer_record[p] = False
                    while self.total_neo4j_number + sub_total_number >= self.total_number * self.transfer_percent:
                        min_r10_p = ""
                        min_r10 = 1e10
                        for p in set(self.transfer_record.keys()):
                            if self.transfer_record[p]:
                                if self.reward[p][1][0] < min_r10:
                                    min_r10_p = p
                                    min_r10 = self.reward[p][1][0]
                        if min_r10_p == '':
                            print("There is no p in Neo4j, directly transfer.")
                            break
                        print("Still ovev data threshold, delete" + min_r10_p + " because its R10 is the min:" + str(min_r10))
                        p_number = int(self.numbers[min_r10_p])
                        self.total_neo4j_number -= p_number
                        self.transfer_record[min_r10_p] = False

                # 将剩下的已transfer的p导入
                for p in self.transfer_record.keys():
                    if self.transfer_record[p]:
                        need_transfer.add(p)

                # delete original transfer record
                for p in self.transfer_record.keys():
                    self.transfer_record[p] = False
                print("need transfer: " + str(need_transfer))
                # 开始transfer
                print("transfering")
                transfer_batch_time = self.transfer_neo4j_import_no_merge(set(need_transfer))
                print("transfer time:" + str(transfer_batch_time))
                for p in need_transfer:
                    self.transfer_record[p] = True
                print("transfer over")

                # 计算奖励和Q表
                print("calculate q and reward")
                t2 = threading.Thread(target=self.neo_query_time, args=(new_cypher, length))
                t2.start()
                t2.join(self.max_time)
                if t2.is_alive():
                    stop_thread(t2)
                    print("Neo4j over time!")
                    neo4j_time = self.max_time
                else:
                    print("neo4j query success")
                    neo4j_time, result_list = self.this_time_neo, self.this_time_neo4j_results
                print("neo4j time:" + str(neo4j_time))
                self.update_after_transfer(new_mysql, neo4j_time, sub_p_rate, last_time_record)
        total_end = time.perf_counter()  # end: Total Time
        q_values = []
        q00 = 0
        q01 = 0
        q10 = 0
        q11 = 0
        for p in self.q.keys():
            q00 += self.q[p][0][0]
            q01 += self.q[p][0][1]
            q10 += self.q[p][1][0]
            q11 += self.q[p][1][1]
        q_values.append(q00)
        q_values.append(q01)
        q_values.append(q10)
        q_values.append(q11)
        print(self.batch_time_list)
        with open("record128.txt", "a", encoding="utf8") as fw:
            fw.write("Q Learning:" + str(self.batch_time_list) + "\n\n")
        return total_end - total_start, q_values, self.total_batch_time

    def transfer_neo4j_import(self, p_list):
        neo4j_home = self.cf.get("rdb-gdb", "neo4j_home")
        start = time.perf_counter()
        db = pymysql.connect(host=self.host, port=int(self.port), user=self.user,
                             passwd=self.password, db=self.db, charset=self.charset)
        cursor = db.cursor()
        f_s = open(neo4j_home + '/import/s.csv', 'w', encoding='utf8')
        f_p = open(neo4j_home + '/import/p.csv', 'w', encoding='utf8')
        f_o = open(neo4j_home + '/import/o.csv', 'w', encoding='utf8')
        f_s.write("subjectId:ID\tname\t:LABEL\n")
        f_o.write("objectId:ID\tname\t:LABEL\n")
        f_p.write(":START_ID\t:END_ID\t:TYPE\n")
        print("transferring p list:" + str(p_list))
        all_need = []
        for p in p_list:
            sql = "SELECT sname, pname, oname FROM " + self.db + self.p_sum[p] + ";"
            try:
                cursor.execute(sql)
                results = cursor.fetchall()
                f_sw = set()
                f_ow = set()
                print(p + str(len(results)))
                for result in results:
                    s_id = str(hash(result[0]))
                    o_id = str(hash(result[2]))
                    if s_id not in f_sw:
                        f_s.write(s_id + '\t"' + result[0].replace('"', '').replace('\\', '') + '"\t"subject"\n')
                        f_sw.add(s_id)
                    if o_id not in f_ow:
                        f_o.write(o_id + '\t"' + result[2].replace('"', '').replace("\r", "")
                                  .replace(" ", "").replace("\n",  "").replace('\\', '') + '"\t"object"\n')
                        f_ow.add(o_id)
                    f_p.write(s_id + "\t" + o_id + "\t\"" + result[1].replace('"', '') + "\"\n")
            except:
                db.ping()
                cursor = db.cursor()
                cursor.execute(sql, (p_list,))
                results = cursor.fetchall()
                print(p + str(len(results)))
                f_sw = set()
                f_ow = set()
                print(p + str(len(results)))
                for result in results:
                    s_id = str(hash(result[0]))
                    o_id = str(hash(result[2]))
                    if s_id not in f_sw:
                        f_s.write(s_id + '\t"' + result[0].replace('"', '').replace('\\', '') + '"\t"subject"\n')
                        f_sw.add(s_id)
                    if o_id not in f_ow:
                        f_o.write(o_id + '\t"' + result[2].replace('"', '').replace("\r", "")
                                  .replace(" ", "").replace("\n", "").replace('\\', '') + '"\t"object"\n')
                        f_ow.add(o_id)
                    f_p.write(s_id + "\t" + o_id + "\t\"" + result[1].replace('"', '').replace('\\', '') + "\"\n")
            results = []
        f_s.close()
        f_p.close()
        f_o.close()
        db.close()
        os.system(
            "rm -rf '" + neo4j_home + "/data/databases/graph2.db' > log.txt")
        os.system("'" + neo4j_home + "/bin/neo4j' restart > log.txt")
        os.system("'" + neo4j_home + "/bin/neo4j' stop > log.txt")
        os.system("'" + neo4j_home + "/bin/neo4j-import' "
                  "--into '" + neo4j_home + "/data/databases/graph2.db' "
                  "--ignore-empty-strings=true "
                  "--skip-bad-entries-logging=true "
                  "--read-buffer-size=128m "
                  "--nodes '" + neo4j_home + "/import/s.csv' "
                  "--nodes '" + neo4j_home + "/import/o.csv' "
                  " --relationships '" + neo4j_home + "/import/p.csv'  "
                  "--delimiter='\t' "
                  "--quote='\"' "
                  "–-ignore-extra-columns=true "
                  "--skip-bad-relationships "
                  "--skip-duplicate-nodes=true "
                  "--bad-tolerance=50000000 ")
                  # " > log.txt")
        # " > /mnt/hgfs/research_data/new/transfer_result.txt")
        os.system("'" + neo4j_home + "/bin/neo4j' start  > log.txt")
        time.sleep(60)
        end = time.perf_counter()
        return end - start

    def transfer_neo4j_import_no_merge(self, p_list):
        neo4j_home = self.cf.get("rdb-gdb", "neo4j_home")
        start = time.perf_counter()
        db = pymysql.connect(host=self.host, port=int(self.port), user=self.user,
                             passwd=self.password, db=self.db, charset=self.charset)
        cursor = db.cursor()
        f_s = open(neo4j_home + '/import/s.csv', 'w', encoding='utf8')
        f_p = open(neo4j_home + '/import/p.csv', 'w', encoding='utf8')
        f_o = open(neo4j_home + '/import/o.csv', 'w', encoding='utf8')
        f_s.write("subjectId:ID\tname\t:LABEL\n")
        f_o.write("objectId:ID\tname\t:LABEL\n")
        f_p.write(":START_ID\t:END_ID\t:TYPE\n")
        print("transferring p list:" + str(p_list))
        format_p = "("
        for p in p_list:
            format_p += "'" + p + "', "
        format_p = format_p.rstrip(", ") + ")"
        sql = "SELECT sname, pname, oname FROM watdiv WHERE pname in {};".format(format_p)
        print(sql)
        try:
            cursor.execute(sql)
            results = cursor.fetchall()
            f_sw = set()
            f_ow = set()
            print(p + str(len(results)))
            for result in results:
                s_id = str(hash(result[0]))
                o_id = str(hash(result[2]))
                if s_id not in f_sw:
                    f_s.write(s_id + '\t"' + result[0].replace('"', '').replace('\\', '') + '"\t"subject"\n')
                    f_sw.add(s_id)
                if o_id not in f_ow:
                    f_o.write(o_id + '\t"' + result[2].replace('"', '').replace("\r", "")
                              .replace(" ", "").replace("\n", "").replace('\\', '') + '"\t"object"\n')
                    f_ow.add(o_id)
                f_p.write(s_id + "\t" + o_id + "\t\"" + result[1].replace('"', '') + "\"\n")
        except:
            db.ping()
            cursor = db.cursor()
            cursor.execute(sql, (p_list,))
            results = cursor.fetchall()
            print(p + str(len(results)))
            f_sw = set()
            f_ow = set()
            print(p + str(len(results)))
            for result in results:
                s_id = str(hash(result[0]))
                o_id = str(hash(result[2]))
                if s_id not in f_sw:
                    f_s.write(s_id + '\t"' + result[0].replace('"', '').replace('\\', '') + '"\t"subject"\n')
                    f_sw.add(s_id)
                if o_id not in f_ow:
                    f_o.write(o_id + '\t"' + result[2].replace('"', '').replace("\r", "")
                              .replace(" ", "").replace("\n", "").replace('\\', '') + '"\t"object"\n')
                    f_ow.add(o_id)
                f_p.write(s_id + "\t" + o_id + "\t\"" + result[1].replace('"', '').replace('\\', '') + "\"\n")
        f_s.close()
        f_p.close()
        f_o.close()
        db.close()
        os.system(
            "rm -rf '" + neo4j_home + "/data/databases/graph2.db' > log.txt")
        os.system("'" + neo4j_home + "/bin/neo4j' restart > log.txt")
        os.system("'" + neo4j_home + "/bin/neo4j' stop > log.txt")
        os.system("'" + neo4j_home + "/bin/neo4j-import' "
                  "--into '" + neo4j_home + "/data/databases/graph2.db' "
                  "--ignore-empty-strings=true "
                  "--skip-bad-entries-logging=true "
                  "--read-buffer-size=128m "
                  "--nodes '" + neo4j_home + "/import/s.csv' "
                  "--nodes '" + neo4j_home + "/import/o.csv' "
                  " --relationships '" + neo4j_home + "/import/p.csv'  "
                  "--delimiter='\t' "
                  "--quote='\"' "
                  "–-ignore-extra-columns=true "
                  "--skip-bad-relationships "
                  "--skip-duplicate-nodes=true "
                  "--bad-tolerance=50000000 ")
                  # " > log.txt")
        # " > /mnt/hgfs/research_data/new/transfer_result.txt")
        os.system("'" + neo4j_home + "/bin/neo4j' start  > log.txt")
        time.sleep(60)
        end = time.perf_counter()
        return end - start

    def _transfer_by_file(self, p_list):
        neo4j_home = self.cf.get("rdb-gdb", "neo4j_home")
        file_home = "/home/sde/BigData_zhanghaoran/original_data/"
        start = time.perf_counter()
        f_s = open(neo4j_home + '/import/s.csv', 'w', encoding='utf8')
        f_p = open(neo4j_home + '/import/p.csv', 'w', encoding='utf8')
        f_o = open(neo4j_home + '/import/o.csv', 'w', encoding='utf8')
        f_s.write("subjectId:ID\tname\t:LABEL\n")
        f_o.write("objectId:ID\tname\t:LABEL\n")
        f_p.write(":START_ID\t:END_ID\t:TYPE\n")
        print("transferring p list:" + str(p_list))
        f_sw = set()
        f_ow = set()
        for p in p_list:
            with open(file_home + "depedia_sum_" + str(self.p_sum[p]) + ".csv", "r", encoding="utf8") as f:
                line = f.readline()
                while line:
                    if p in line:
                        split = line.split("\t")
                        s_id = str(hash(split[0]))
                        o_id = str(hash(split[2]))
                        if s_id not in f_sw:
                            f_s.write(s_id + '\t"' + split[0].replace('"', '').replace('\\', '') + '"\t"subject"\n')
                            f_sw.add(s_id)
                        if o_id not in f_ow:
                            f_o.write(o_id + '\t"' + split[2].replace('"', '').replace("\r", "")
                                      .replace(" ", "").replace("\n", "").replace('\\', '') + '"\t"object"\n')
                            f_ow.add(o_id)
                        f_p.write(s_id + "\t" + o_id + "\t\"" + split[1].replace('"', '') + "\"\n")
                    line = f.readline()
        f_s.close()
        f_p.close()
        f_o.close()
        os.system(
            "rm -rf '" + neo4j_home + "/data/databases/graph2.db' > log.txt")
        os.system("echo \'222762\' | sudo -S '" + neo4j_home + "/bin/neo4j' restart > log.txt")
        os.system("echo \'222762\' | sudo -S '" + neo4j_home + "/bin/neo4j' stop > log.txt")
        os.system("'" + neo4j_home + "/bin/neo4j-import' "
                  "--into '" + neo4j_home + "/data/databases/graph2.db' "
                  "--ignore-empty-strings=true "
                  "--skip-bad-entries-logging=true "
                  "--read-buffer-size=128m "
                  "--nodes '" + neo4j_home + "/import/s.csv' "
                  "--nodes '" + neo4j_home + "/import/o.csv' "
                  " --relationships '" + neo4j_home + "/import/p.csv'  "
                  "--delimiter='\t' "
                  "--quote='\"' "
                  "–-ignore-extra-columns=true "
                  "--skip-bad-relationships "
                  "--skip-duplicate-nodes=true "
                  "--bad-tolerance=50000000 "
                  "--high-io=true")
        # " > log.txt")
        # " > /mnt/hgfs/research_data/new/transfer_result.txt")
        os.system("echo \'222762\' | sudo -S '" + neo4j_home + "/bin/neo4j' start  > log.txt")
        time.sleep(60)
        graph = Graph(self.cf.get("rdb-gdb", "graph_uri"), username='neo4j', password='123456', secure=False,
                      bolt=False)  # neo4j连接
        graph.run("CREATE INDEX ON:subject(name)")
        graph.run("CREATE INDEX ON:object(name)")
        os.system("echo \'222762\' | sudo -S '" + neo4j_home + "/bin/neo4j' restart  > log.txt")
        time.sleep(60)
        end = time.perf_counter()
        return end - start


if __name__ == '__main__':
    print(sys.argv)
    q = QLearning(sys.argv[1].replace("\r", ""))
    q.q_learning()
    q.q_learning()


