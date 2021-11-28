import numpy as np
import psycopg2
import psycopg2.extras
import psycopg2.pool
import time


workload_path = "workloads/workload"


class REWARD:
    def __init__(self):
        self.workload_path = workload_path

        try:
            simple_conn_pool = psycopg2.pool.SimpleConnectionPool(minconn=1, maxconn=5, dbname="test",
                                                                  user="postgres",
                                                                  password="postgres",
                                                                  host="127.0.0.1",
                                                                  port="5432")
            self.conn = simple_conn_pool.getconn()
            self.cur = self.conn.cursor()
        except psycopg2.DatabaseError as e:
            print("Reward Could not connect to DataBase Server ~T^T\n", e)

    def test(self):
        start_time = time.time()
        workload = open(workload_path, "r")
        for sql in workload:
            self.cur.execute(sql)
        self.conn.commit()
        end_time = time.time()
        return 1000/(end_time - start_time)