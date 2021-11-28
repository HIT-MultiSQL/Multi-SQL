import sys

from user_behavior_workload import UserBehaviorMeta, UserBehaviorWorkload, UserBehaviorWriteQuery, UserBehaviorQ1, \
    UserBehaviorQ2, UserBehaviorQ3
from user_inform_workload import UserInformMeta, UserInformWorkload, UserInformQ2, UserInformQ3, UserInformQ4, \
    UserInformQ5, UserInformWriteQuery
from write_model import WriteModel
from read_model import ReadModel
from workload import *
from columngroup_recommend import ColGroupRecommend
from typing import List
from cs_read_model import ColumnReadModel

break_cost = False
break_col_group = False


def remove_key(partial_result, table):
    result = []
    for col_group in partial_result:
        tmp = []
        for column in col_group:
            if column not in table.keys:
                tmp.append(column)
        if len(tmp) > 0:
            result.append(tmp)
    return result


def predict_format(table, col_groups: List[List[str]], query_column: List[str], engine: str) -> (List[int], List[int]):
    necessary_col_group = []
    for col_group in col_groups:
        for column in col_group:
            if column in query_column:
                necessary_col_group.append(col_group)
                break

    row_size = []
    value_field = []
    for col_group in necessary_col_group:
        row_size.append(0)
        value_field.append(0)
        row_size[-1] += table.key_len[engine]
        row_size[-1] += table.get_row_length(col_group, engine)
        value_field[-1] += len(col_group)
    return row_size, value_field


def loadWorkFlow():
    inform_w1 = UserInformWorkload(0, 100, 10, 10000, 10000)
    workloads1 = [inform_w1]
    behavior_w1 = UserBehaviorWorkload(10000, 10000, 100, 10)
    workloads2 = [behavior_w1]
    return workloads1, workloads2


def recommend(inform_workloads, behavior_workloads):
    user_inform = UserInformMeta()
    user_behavior = UserBehaviorMeta()
    rs_write_model = WriteModel("rs")
    cs_write_model = WriteModel("cs")
    lsm_write_model = WriteModel("lsm")
    rs_seq_read_model = ReadModel("rs_seq")
    rs_rand_read_model = ReadModel("rs_rand")
    lsm_seq_read_model = ReadModel("lsm_seq")
    lsm_rand_read_model = ReadModel("lsm_rand")
    cs_read_model = ColumnReadModel()


    inform_rec = ColGroupRecommend(user_inform)
    for i in range(len(inform_workloads)):
        workload = inform_workloads[i]
        # print("%d other method" % i)
        # column_groups = inform_rec.recommend_tile(workload, 3)
        # column_groups = remove_key(column_groups, user_inform)
        # for column_group in column_groups:
        #     print(column_group)
        print("Artificial decision method: ")
        flag = 0
        if  workload.write_query > 10000:
            if workload.q2 > 5000:
                flag = flag | 1
            if workload.q3 > 5000:
                flag = flag | 2
            if workload.q4 > 5000:
                flag = flag | 4
            if workload.q5 > 5000:
                flag = flag | 8
            c = 0
            while flag != 0 :
                flag = flag & (flag - 1)
                c = c + 1
            if c >= 2 :
                print("mysql [['NAME', 'AGE', 'GENDER', 'OCCUPATION'], ['REGISTER_DATE']]")
            else:
                print("mysql [['NAME', 'AGE', 'GENDER', 'OCCUPATION', 'REGISTER_DATE']]")
        else:
            print("column_store")


        print("my method: ")
        best = {}
        for j in range(3):
            workload.reset()
            column_groups = inform_rec.recommend(workload, j + 1)
            column_groups = remove_key(column_groups, user_inform)
            if break_col_group:
                for column_group in column_groups:
                    print(column_group)

            workload.reset()
            # seq wt
            sum_cost = 0
            if workload.write_query > 0:
                cost = 0
                row_size, value_field = predict_format(user_inform, column_groups, UserInformWriteQuery().columns, 'wt')
                for k in range(len(row_size)):
                    single = rs_write_model.predict(row_size=row_size[k], key_field=1, value_field=value_field[k],
                                                    total_rows=workload.write_query, engine="wt")
                    cost += single[0] * workload.write_query
                if break_cost:
                    print("WQ cost:", cost)
                sum_cost += cost
            if workload.q2 > 0:
                cost = 0
                row_size, value_field = predict_format(user_inform, column_groups, UserInformQ2().columns, 'wt')
                for k in range(len(row_size)):
                    single = rs_seq_read_model.predict(row_size=row_size[k], key_field=1, value_field=value_field[k],
                                                       read_rows=1, is_random=0, current_rows=10000)
                    cost += single * workload.q2 * UserInformQ2().affect_rows
                if break_cost:
                    print("Q2 cost:", cost)
                sum_cost += cost
            if workload.q3 > 0:
                cost = 0
                row_size, value_field = predict_format(user_inform, column_groups, UserInformQ3().columns, 'wt')
                for k in range(len(row_size)):
                    single = rs_seq_read_model.predict(row_size=row_size[k], key_field=1, value_field=value_field[k],
                                                       read_rows=1, is_random=0, current_rows=10000)
                    cost += single * workload.q3 * UserInformQ3().affect_rows
                if break_cost:
                    print("Q3 cost:", cost)
                sum_cost += cost
            if workload.q4 > 0:
                cost = 0
                row_size, value_field = predict_format(user_inform, column_groups, UserInformQ4().columns, 'wt')
                for k in range(len(row_size)):
                    single = rs_seq_read_model.predict(row_size=row_size[k], key_field=1, value_field=value_field[k],
                                                       read_rows=1, is_random=0, current_rows=10000)
                    cost += single * workload.q4 * UserInformQ4().affect_rows
                if break_cost:
                    print("Q4 cost:", cost)
                sum_cost += cost
            if workload.q5 > 0:
                cost = 0
                row_size, value_field = predict_format(user_inform, column_groups, UserInformQ5().columns, 'wt')
                for k in range(len(row_size)):
                    single = rs_seq_read_model.predict(row_size=row_size[k], key_field=1, value_field=value_field[k],
                                                       read_rows=1, is_random=0, current_rows=10000)
                    cost += single * workload.q5 * UserInformQ5().affect_rows
                if break_cost:
                    print("Q5 cost:", cost)
                sum_cost += cost
            if break_col_group:
                print("wt Total cost:", sum_cost)
            best["mysql " + str(column_groups)] = sum_cost
        # column store
        if workload.q4 == 0:
            sum_cost = 0
            if workload.write_query > 0:
                single = cs_write_model.predict(row_size=39, key_field=1, value_field=5,
                                                total_rows=workload.write_query, engine="wt")
                cost = single[0] * workload.write_query
                sum_cost += cost
            if workload.q2 > 0:
                single = cs_read_model.predict([5, 8], 10000, [1, 2])
                cost = single * workload.q2
                sum_cost += cost
            if workload.q3 > 0:
                single = cs_read_model.predict([5, 8], 10000, [1, 2])
                cost = single * workload.q3
                sum_cost += cost
            if workload.q5 > 0:
                single = cs_read_model.predict([5, 4], 10000, [1, 2])
                cost = single * workload.q5
                sum_cost += cost
            best["column_store"] = sum_cost
        min_cost = -1
        min_cost_format = ""
        for k in best.keys():
            if min_cost == -1:
                min_cost = best[k]
                min_cost_format = k
            elif best[k] < min_cost:
                min_cost = best[k]
                min_cost_format = k
        print( min_cost_format)

    behavior_rec = ColGroupRecommend(user_behavior)
    for i in range(len(behavior_workloads)):
        workload = behavior_workloads[i]
        # print("%d other method" % i)
        # column_groups = behavior_rec.recommend_tile(workload, 3)
        # column_groups = remove_key(column_groups, user_behavior)
        # for column_group in column_groups:
        #     print(column_group)
        print("Artificial decision method: ")
        flag = 0
        if  workload.write_query > 10000:
            if workload.q1 > 5000:
                flag = flag | 1
            if workload.q2 > 5000:
                flag = flag | 2
            if workload.q3 > 5000:
                flag = flag | 4
            c = 0
            while flag != 0:
                flag = flag & (flag - 1)
                c = c + 1
            if c >= 2:
                print("mysql [['UID', LOG_TIME], ['IP', 'DEVICE']]")
            else:
                print("mysql [['UID', LOG_TIME, 'IP', 'DEVICE']]")
        else:
            print("column_store")

        print("my method: ")
        best = {}
        for j in range(3):
            workload.reset()
            column_groups = behavior_rec.recommend(workload, j + 1)
            column_groups = remove_key(column_groups, user_behavior)
            if break_col_group:
                for column_group in column_groups:
                    print(column_group)

            workload.reset()
            # seq wt
            sum_cost = 0
            if workload.write_query > 0:
                cost = 0
                row_size, value_field = predict_format(user_behavior, column_groups, UserBehaviorWriteQuery().columns, 'wt')
                for k in range(len(row_size)):
                    single = rs_write_model.predict(row_size=row_size[k], key_field=1, value_field=value_field[k],
                                                    total_rows=workload.write_query, engine="wt")
                    cost += single[0] * workload.write_query
                if break_cost:
                    print("WQ cost:", cost)
                sum_cost += cost
            if workload.q1 > 0:
                cost = 0
                row_size, value_field = predict_format(user_behavior, column_groups, UserBehaviorQ1().columns, 'wt')
                for k in range(len(row_size)):
                    single = rs_seq_read_model.predict(row_size=row_size[k], key_field=1, value_field=value_field[k],
                                                       read_rows=1, is_random=0, current_rows=100000)
                    cost += single * workload.q1 * UserBehaviorQ1().affect_rows
                if break_cost:
                    print("Q1 cost:", cost)
                sum_cost += cost
            if workload.q2 > 0:
                cost = 0
                row_size, value_field = predict_format(user_behavior, column_groups, UserBehaviorQ2().columns, 'wt')
                for k in range(len(row_size)):
                    single = rs_seq_read_model.predict(row_size=row_size[k], key_field=1, value_field=value_field[k],
                                                       read_rows=1, is_random=0, current_rows=100000)
                    cost += single * workload.q2 * UserBehaviorQ2().affect_rows
                if break_cost:
                    print("Q2 cost:", cost)
                sum_cost += cost
            if workload.q3 > 0:
                cost = 0
                row_size, value_field = predict_format(user_behavior, column_groups, UserBehaviorQ3().columns, 'wt')
                for k in range(len(row_size)):
                    single = rs_seq_read_model.predict(row_size=row_size[k], key_field=1, value_field=value_field[k],
                                                       read_rows=1, is_random=0, current_rows=100000)
                    cost += single * workload.q3 * UserBehaviorQ3().affect_rows
                if break_cost:
                    print("Q3 cost:", cost)
                sum_cost += cost
            if break_col_group:
                print("wt Total cost:", sum_cost)
            best["mysql " + str(column_groups)] = sum_cost
        # column store
        if True:
            sum_cost = 0
            if workload.write_query > 0:
                single = cs_write_model.predict(row_size=49, key_field=1, value_field=4,
                                                        total_rows=workload.write_query, engine="wt")
                cost = single[0] * workload.write_query
                sum_cost += cost
            if workload.q1 > 0:
                single = cs_read_model.predict([5, 9, 13], 100000, [2, 2, 2])
                cost = single * workload.q1
                sum_cost += cost
            if workload.q2 > 0:
                single = cs_read_model.predict([5, 9], 100000, [2, 2])
                cost = single * workload.q2
                sum_cost += cost
            if workload.q3 > 0:
                single = cs_read_model.predict([5, 9], 100000, [2, 2])
                cost = single * workload.q3
                sum_cost += cost
            best["column_store"] = sum_cost
        min_cost = -1
        min_cost_format = ""
        for k in best.keys():
            if min_cost == -1:
                min_cost = best[k]
                min_cost_format = k
            elif best[k] < min_cost:
                min_cost = best[k]
                min_cost_format = k
        print( min_cost_format)


if __name__ == '__main__':
    # table = LineItemMeta()
    # w1 = Workload(9000000, 10000, 10000, 0, 0)
    # w2 = Workload(3000000, 30000, 30000, 5, 5)
    # w3 = Workload(0, 10000, 10000, 15, 15)
    # w4 = Workload(0, 0, 0, 30, 30)
    # workloads = [w1, w2, w3, w4]

    inform_workloads, behavior_workloads = loadWorkFlow()
    recommend(inform_workloads, behavior_workloads)

    # print("loading model")
    # rs_write_model = WriteModel("rs")
    # cs_write_model = WriteModel("cs")
    # lsm_write_model = WriteModel("lsm")
    # rs_seq_read_model = ReadModel("rs_seq")
    # rs_rand_read_model = ReadModel("rs_rand")
    # lsm_seq_read_model = ReadModel("lsm_seq")
    # lsm_rand_read_model = ReadModel("lsm_rand")
    # cs_read_model = ColumnReadModel()
    #
    # rec = ColGroupRecommend(table)
    # for i in range(len(workloads)):
    #     workload = workloads[i]
    #
    #     print("%d other method" % i)
    #     column_groups = rec.recommend_tile(workload, 3)
    #     column_groups = remove_key(column_groups, table)
    #     for column_group in column_groups:
    #         print(column_group)
    #
    #     print("\n%d my method" % i)
    #     print("-------------------------------")
    #     best = {}
    #     for j in range(3):
    #         workload.reset()
    #         column_groups = rec.recommend(workload, j + 1)
    #         column_groups = remove_key(column_groups, table)
    #         if break_col_group:
    #             for column_group in column_groups:
    #                 print(column_group)
    #
    #         workload.reset()
    #         # seq wt
    #         sum_cost = 0
    #         if workload.write_query > 0:
    #             cost = 0
    #             row_size, value_field = predict_format(table, column_groups, WriteQuery().columns, 'wt')
    #             for k in range(len(row_size)):
    #                 single = rs_write_model.predict(row_size=row_size[k], key_field=4, value_field=value_field[k], total_rows=workload.write_query, engine="wt")
    #                 cost += single[0] * workload.write_query
    #             if break_cost:
    #                 print("WQ cost:", cost)
    #             sum_cost += cost
    #         if workload.read_query_1 > 0:
    #             cost = 0
    #             row_size, value_field = predict_format(table, column_groups, ReadQuery1().columns, 'wt')
    #             for k in range(len(row_size)):
    #                 single = rs_seq_read_model.predict(row_size=row_size[k], key_field=4, value_field=value_field[k], read_rows=1, is_random=0, current_rows=9000000)
    #                 cost += single * workload.read_query_1 * ReadQuery1().affect_rows
    #             if break_cost:
    #                 print("RQ1 cost:", cost)
    #             sum_cost += cost
    #         if workload.read_query_2 > 0:
    #             cost = 0
    #             row_size, value_field = predict_format(table, column_groups, ReadQuery2().columns, 'wt')
    #             for k in range(len(row_size)):
    #                 single = rs_seq_read_model.predict(row_size=row_size[k], key_field=4, value_field=value_field[k], read_rows=1, is_random=0, current_rows=9000000)
    #                 cost += single * workload.read_query_2 * ReadQuery2().affect_rows
    #             if break_cost:
    #                 print("RQ2 cost:", cost)
    #             sum_cost += cost
    #         if workload.q3 > 0:
    #             cost = 0
    #             row_size, value_field = predict_format(table, column_groups, Q3().columns, 'wt')
    #             for k in range(len(row_size)):
    #                 single = rs_seq_read_model.predict(row_size=row_size[k], key_field=4, value_field=value_field[k], read_rows=1, is_random=0, current_rows=9000000)
    #                 cost += single * workload.q3 * Q3().affect_rows
    #             if break_cost:
    #                 print("Q3 cost:", cost)
    #             sum_cost += cost
    #         if workload.q5 > 0:
    #             cost = 0
    #             row_size, value_field = predict_format(table, column_groups, Q5().columns, 'wt')
    #             for k in range(len(row_size)):
    #                 single = rs_seq_read_model.predict(row_size=row_size[k], key_field=4, value_field=value_field[k], read_rows=1, is_random=0, current_rows=9000000)
    #                 cost += single * workload.q5 * Q5().affect_rows
    #             if break_cost:
    #                 print("Q5 cost:", cost)
    #             sum_cost += cost
    #         if break_col_group:
    #             print("wt Total cost:", sum_cost)
    #         best["wt "+str(column_groups)] = sum_cost
    #         # seq rocks
    #         sum_cost = 0
    #         if workload.write_query > 0:
    #             cost = 0
    #             row_size, value_field = predict_format(table, column_groups, WriteQuery().columns, 'rocks')
    #             for k in range(len(row_size)):
    #                 single = lsm_write_model.predict(row_size=row_size[k], key_field=4, value_field=value_field[k], total_rows=workload.write_query, engine="wt")
    #                 cost += single[0] * workload.write_query
    #             if break_cost:
    #                 print("WQ cost:", cost)
    #             sum_cost += cost
    #         if workload.read_query_1 > 0:
    #             cost = 0
    #             row_size, value_field = predict_format(table, column_groups, ReadQuery1().columns, 'rocks')
    #             for k in range(len(row_size)):
    #                 single = lsm_seq_read_model.predict(row_size=row_size[k], key_field=4, value_field=value_field[k], read_rows=1, is_random=0, current_rows=9000000)
    #                 cost += single * workload.read_query_1 * ReadQuery1().affect_rows
    #             if break_cost:
    #                 print("RQ1 cost:", cost)
    #             sum_cost += cost
    #         if workload.read_query_2 > 0:
    #             cost = 0
    #             row_size, value_field = predict_format(table, column_groups, ReadQuery2().columns, 'rocks')
    #             for k in range(len(row_size)):
    #                 single = lsm_seq_read_model.predict(row_size=row_size[k], key_field=4, value_field=value_field[k], read_rows=1, is_random=0, current_rows=9000000)
    #                 cost += single * workload.read_query_2 * ReadQuery2().affect_rows
    #             if break_cost:
    #                 print("RQ2 cost:", cost)
    #             sum_cost += cost
    #         if workload.q3 > 0:
    #             cost = 0
    #             row_size, value_field = predict_format(table, column_groups, Q3().columns, 'rocks')
    #             for k in range(len(row_size)):
    #                 single = lsm_seq_read_model.predict(row_size=row_size[k], key_field=4, value_field=value_field[k], read_rows=1, is_random=0, current_rows=9000000)
    #                 cost += single * workload.q3 * Q3().affect_rows
    #             if break_cost:
    #                 print("Q3 cost:", cost)
    #             sum_cost += cost
    #         if workload.q5 > 0:
    #             cost = 0
    #             row_size, value_field = predict_format(table, column_groups, Q5().columns, 'rocks')
    #             for k in range(len(row_size)):
    #                 single = lsm_seq_read_model.predict(row_size=row_size[k], key_field=4, value_field=value_field[k], read_rows=1, is_random=0, current_rows=9000000)
    #                 cost += single * workload.q5 * Q5().affect_rows
    #             if break_cost:
    #                 print("Q5 cost:", cost)
    #             sum_cost += cost
    #         if break_col_group:
    #             print("rocks Total cost:", sum_cost)
    #         best["rocks "+str(column_groups)] = sum_cost
    #         if break_col_group:
    #             print("-------------------------------")
    #     # column store
    #     if workload.read_query_1 == 0 and workload.read_query_2 == 0:
    #         sum_cost = 0
    #         if workload.write_query > 0:
    #             single = cs_write_model.predict(row_size=128, key_field=4, value_field=12,
    #                                             total_rows=workload.write_query, engine="wt")
    #             cost = single[0] * workload.write_query
    #             sum_cost += cost
    #         if workload.q3 > 0:
    #             single = cs_read_model.predict([4, 9, 10, 4], 11997996, [1, 2, 2, 2])
    #             cost = single * workload.q3
    #             sum_cost += cost
    #         if workload.q5 > 0:
    #             single = cs_read_model.predict([4, 4, 9, 4], 11997996, [1, 1, 2, 2])
    #             cost = single * workload.q5
    #             sum_cost += cost
    #         best["column_store"] = sum_cost
    #     min_cost = -1
    #     min_cost_format = ""
    #     for k in best.keys():
    #         if min_cost == -1:
    #             min_cost = best[k]
    #             min_cost_format = k
    #         elif best[k] < min_cost:
    #             min_cost = best[k]
    #             min_cost_format = k
    #     print(">>> rec:", min_cost_format)
    #     print("-------------------------------")