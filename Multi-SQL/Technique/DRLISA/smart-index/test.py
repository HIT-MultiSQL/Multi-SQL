import numpy as np
from env import DataBase
from RL_brain import DuelingDQN


workload_path = "workload/workload"


env = DataBase()
RL = DuelingDQN(env.n_actions, env.n_features,
                  learning_rate=0.001,
                  reward_decay=0.7,
                  e_greedy=0.9
                  )
RL.load_model()
env.load_cache()
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

def optimal_index(wl):
    mr = 0.0
    ss = None
    for episode in range(100):
        observation = env.reset(wl = wl, idx_id = np.random.randint(0, 3), fillfactor = 10 + np.random.randint(0, 2) * 45)
        nr = 0.0
        while True:
            if nr > mr:
                ss = str((observation * 100 + 0.5).astype(int).tolist())
                mr = nr
            action = RL.choose_action(observation)
            observation_, reward, done = env.step(action)
#             print(action, env.action_space[action], observation, observation_, reward)
            nr += reward
            if done:
#                 print('done.')
                break
            observation = observation_
    return ss

def test_throughput(state):
    env.db_init_table()
    print(state)
    state = np.array(state) / 100
    env.reset(state[:-2], int(state[-2] + 0.5), state[-1])
    print(1000/env.get_throughput(use_cache=True))

def test_state(state, keep_state=False):
    print("智能方法：\n")
    test_throughput(state)
    print("人工经验方法：\n")
    if state[3]>state[0] and state[3]>state[2]:
        state[-2] = 200
    if state[0]>state[2]:
        state[-2] = 100
    else:
        state[-2] = 0
    state[-1] = (1-state[3])*100*100
    test_throughput(state)
    
    print("default：\n")
    state[-2] = np.random.randint(0,2) * 100
    state[-1] = 1000
    test_throughput(state)

def test_wl(wl):
    state = optimal_index(wl)
    state = state[1:-1].split(',')
    for i in range(len(state)):
        state[i]=int(state[i])
    test_state(state)

state = test_wl(generate_wl())