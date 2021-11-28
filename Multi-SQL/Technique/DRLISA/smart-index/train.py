# %load trainer.py
#!/usr/bin/env python

import numpy as np
from env import DataBase
from RL_brain import DuelingDQN


workload_path = "workload/workload"


# caculate wl
def generate_wl():
        workload = open(workload_path, "r")
        read_num = 0
        update_num = 0
        scan_num = 0
        insert_num = 0
        for line in workload:
            sql = line.lower().split(" ")
            if "insert" in sql:
                insert_num += 1
            if "update" in sql:
                update_num += 1
            if "select" in sql:
                if "=" in sql:
                    read_num += 1
                if ">" in sql:
                    scan_num += 1
                if "<" in sql:
                    scan_num += 1
                if "between" in sql:
                    scan_num += 1
        operation_sum = read_num + update_num + scan_num + insert_num
        readproportion = read_num/operation_sum
        updateproportion = update_num/operation_sum
        scanproportion = scan_num/operation_sum
        insertproportion = insert_num/operation_sum
        wl = np.array([readproportion, updateproportion, scanproportion, insertproportion])
        return wl

        
# load workload to "workload/workload"
def load_workload(path):
    workload_temp = open(path, "r")
    workload = open(workload_path, "w")
    file_temp = ""
    for line in workload_temp:
        file_temp +=line
    workload.write(file_temp)
    workload_temp.close()
    workload.close()
    

# wl=[readproportion,updateproportion,scanproportion,insertproportion]
def trainer(env, RL, wl, num=1000):
    print('Training begin: ')
    step = 0
    for episode in range(num):
        # initial observation
        observation = env.reset(wl = wl, idx_id = np.random.randint(0, 3), fillfactor = 10 + np.random.randint(0, 2) * 45)

        while True:
            # RL choose action based on observation
            action = RL.choose_action(observation)

            # RL take action and get next observation and reward
            observation_, reward, done = env.step(action)
            print(episode, step, observation, observation_, action, reward)
            
            RL.store_transition(observation, action, reward, observation_)

            if (step > 200) and (step % 5 == 0):
                RL.learn()
            
            # break while loop when end of this episode
            if done:
                break

            # swap observation
            observation = observation_

            step += 1

    RL.save_model()
    env.save_cache()
    print('Training over: ')
    
env = DataBase()
RL = DuelingDQN(env.n_actions, env.n_features,
                  learning_rate=0.001,
                  reward_decay=0.7,
                  e_greedy=0.7,
                  replace_target_iter=200,
                  memory_size=50000,
                  # output_graph=True
                  )
#env.save_cache()
#RL.save_model()
env.load_cache()
RL.load_model()
load_workload("workload/workloada")
trainer(env, RL, generate_wl(), 1000)