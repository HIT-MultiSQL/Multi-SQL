#!/usr/bin/env python
"""
Reinforcement Learning NoSQL Database Example

Reward function used the throughput under different workload

This script is the enviroment part of this example.
The RL is in DQN.py
"""

import psycopg2
import psycopg2.extras
import psycopg2.pool
import numpy as np
import ycsb
import json
import reward

index_name = ['btree', 'hash', 'lsm']
data_path = "data"
workload_path = "workloads/workload"

class DataBase(object):
    def __init__(self):
        self.conn = self._db_connect()
        print(self.conn)
        self.cur = self.conn.cursor()
        
        self.my_reward = reward.REWARD()
 
        self.cache = {}
        self.last_throughput = 0
   
        #default fillfactor 90, range[10~100], +1, +5, +10, +25, +1%, +5%, +10%, +25%
        self.action_space = [
            'btree',
            'btree fillfactor up 45',
            'btree fillfactor down 45',
            'hash',
            'hash fillfactor up 45',
            'hash fillfactor down 45',
            'lsm',
        ]
        self.db_init_table()
        #self.reset()
        self.n_actions = len(self.action_space)
        self.n_features = 6
        print('Env ready.')
        #Testing
#         print('Award Test:\n', self._db_test())
#         print('Action Test:\n')
#         self.step(2)


 
    def get_workload(self):
        return self.state[:-2]
        
    def get_idx_id(self):
        return int(self.state[-2] + 0.5)
    
    def get_fillfactor(self):
        return self.state[-1]
    
    def set_workload(self, wl):
        for i in range(4):
            self.state[i] = wl[i]
    
    def set_idx_id(self, idx_id):
        self.state[-2] = idx_id
    
    def set_fillfactor(self, fillfactor):
        self.state[-1] = fillfactor
    
    # 重置工作环境,并更改数据库索引计算此时的吞吐量作为last_throughput
    def reset(self, wl = np.array([0, 0, 0, 1]), idx_id = 0, fillfactor = 10, count=1000, need_reward=True):
        # Workload distribute, Index[0-2]:[btree, hash, lsm], Fillfactor
        # [readproportion, updateproportion, scanproportion, insertproportion]
        
        self.state = np.hstack((wl, idx_id, fillfactor))
        self._db_index_change()
        self.used_state = []
        if need_reward:
            self.last_throughput = self.get_throughput()
        else:
            self.last_throughput = 0.0
        self.used_state.append(self.get_str_state())
        return self.state.copy()

    #connect database
    def _db_connect(self):
        try:
            simple_conn_pool = psycopg2.pool.SimpleConnectionPool(minconn=1, maxconn=5,dbname="test",
                                  user="postgres",
                                  password="postgres",
                                  host="127.0.0.1",
                                  port="5432")
            conn = simple_conn_pool.getconn()
            return conn
        except psycopg2.DatabaseError as e:
            print("Could not connect to DataBase Server ~T^T\n", e)

    def db_init_table(self):
        try:
            self.cur.execute("DROP TABLE IF EXISTS usertable;")
        except:
            self.conn.rollback()
            print("It's foreign table.")
        try:
            self.cur.execute("DROP FOREIGN TABLE IF EXISTS usertable;")
        except:
            self.conn.rollback()
            print("It's postgresql table.")
        self.cur.execute("DROP TABLE IF EXISTS pg_table;")
        self.cur.execute("CREATE TABLE pg_table (YCSB_KEY VARCHAR(255) PRIMARY KEY not NULL, YCSB_VALUE VARCHAR(255) not NULL);")
        self.cur.execute("DROP FOREIGN TABLE IF EXISTS kv_table;")
        self.cur.execute("CREATE FOREIGN TABLE kv_table(YCSB_KEY VARCHAR(255) not NULL, YCSB_VALUE VARCHAR(255) not NULL) SERVER kv_server")
        self.conn.commit()
        data = open(data_path, "r")
        js_data = data.read()
        dic_data = json.loads(js_data)
        for key in dic_data:
            self.cur.execute("INSERT INTO pg_table (YCSB_KEY,YCSB_VALUE) VALUES ('%s', '%s')"%(key, dic_data[key]));
            self.cur.execute("INSERT INTO kv_table (YCSB_KEY,YCSB_VALUE) VALUES ('%s', '%s')"%(key, dic_data[key]));
        self.conn.commit()
        
    
    def get_str_state(self):
        ss = self.state.copy()
        if ss[-2]==2:
            ss[-1]=10
        return str((ss * 100 + 0.5).astype(int).tolist())
    
    def get_throughput(self, use_cache=True):
        now_state = self.get_str_state()
#        print(now_state)
        old = self.cache.get(now_state)
        if old != None and old[0] >= 1 and use_cache:
            throughput = old[1]
        else:
            if index_name[self.get_idx_id()] != 'lsm':
                # self.cur.execute("TRUNCATE pg_table RESTART IDENTITY;")
                self.cur.execute("ALTER TABLE pg_table RENAME TO usertable;")
            else:
                self.cur.execute("ALTER TABLE kv_table RENAME TO usertable;")
            self.conn.commit()
            throughput = self.my_reward.test()
#             print(now_state, throughput)
            if index_name[self.get_idx_id()] != 'lsm':
                self.cur.execute("ALTER TABLE usertable RENAME TO pg_table;")
            else:
                self.cur.execute("ALTER TABLE usertable RENAME TO kv_table;")
            self.conn.commit()
            if use_cache:
                if old == None:
                    self.cache[now_state] = [1, throughput]
                else:
                    self.cache[now_state][0] += 1
                    self.cache[now_state][1] = (self.cache[now_state][1] * (self.cache[now_state][0] - 1) + throughput) \
                                      / self.cache[now_state][0]
        return throughput
    
    #get reward funcation
    def get_reward(self):
        throughput = self.get_throughput()
        if throughput < self.last_throughput:
            reward = 0.0
        else:
            reward = np.log((throughput - self.last_throughput) / 10.0 + 1.0)
        self.last_throughput = throughput
        return reward
    
    def _db_index_change(self):
        idx_id, new_ff = self.get_idx_id(), self.get_fillfactor()
        if index_name[idx_id] == 'lsm':
            return
        self.cur.execute("DROP INDEX IF EXISTS idx;")
        self.conn.commit()
        self.cur.execute(
            "CREATE INDEX idx ON pg_table USING " +
            index_name[idx_id] + 
            " (YCSB_KEY) WITH (fillfactor = " +
            str(int(new_ff + 0.5)) + 
            ");"
        )
        self.conn.commit()

    #do action
    def step(self, action_id, need_reward=True):
        fillfactor = self.get_fillfactor()
        action_split = self.action_space[action_id].split()
        idx_id = index_name.index(action_split[0])
        if len(action_split) > 1:
            d = float(action_split[3])
            fillfactor = fillfactor + (1 if action_split[2] == 'up' else -1) * d
            if fillfactor < 10:
                fillfactor = 10
            if fillfactor > 100:
                fillfactor = 100
        # next state
        idx_change_cost = -0.1 if self.get_idx_id() != idx_id or self.get_fillfactor() != fillfactor else 0.000
        self.set_idx_id(idx_id)
        self.set_fillfactor(fillfactor)
        self._db_index_change() 
        # reward function
        if need_reward:
            reward = self.get_reward()
            if reward < 0.001:
                reward += idx_change_cost
        else:
            reward = 0
        done = (self.get_str_state() in self.used_state)
        self.used_state.append(self.get_str_state())
        return self.state.copy(), reward, done
        
    def save_cache(self):
        with open('cache.json', 'w') as ff:
            ff.write(json.dumps(self.cache))
        print('Cache saved.')
    
    def load_cache(self):
        with open('cache.json', 'r') as ff:
            self.cache = json.loads(ff.read())
        print('Cache loaded.')


if __name__ == "__main__":
    print('test env\n')
    env = DataBase()
