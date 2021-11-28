from typing import List
from workload import Query


class UserBehaviorMeta:
    def __init__(self):
        self.columns = ['ID', 'UID', 'LOG_TIME', 'IP', 'DEVICE']
        self.keys = ['ID']
        self.field_len = {"wt": [6, 5, 9, 16, 13],
                          "rocks": [6, 5, 9, 16, 13]}
        self.key_len = {"wt": 6, "rocks": 6}

    def get_row_length(self, column_no_list: List[int or str], engine: str) -> int:
        ret = 0
        for no in column_no_list:
            if type(no) is int:
                ret += self.field_len[engine][no]
            elif type(no) is str:
                ret += self.field_len[engine][self.get_column_no(no)]
        return ret

    def get_column_no(self, column_name: str) -> int:
        for i in range(len(self.columns)):
            if self.columns[i] == column_name:
                return i
        raise Exception("column name " + column_name + " not found")


class UserBehaviorQ1(Query):
    def __init__(self):
        columns = ['UID', 'LOG_TIME', 'DEVICE']
        Query.__init__(self, columns, 3000 * len(columns), 3000)


class UserBehaviorQ2(Query):
    def __init__(self):
        columns = ['UID', 'LOG_TIME']
        Query.__init__(self, columns, 5000 * len(columns), 5000)


class UserBehaviorQ3(Query):
    def __init__(self):
        columns = ['UID', 'LOG_TIME']
        Query.__init__(self, columns, 10000 * len(columns), 10000)


class UserBehaviorWriteQuery(Query):
    def __init__(self):
        columns = ['ID', 'UID', 'LOG_TIME', 'IP', 'DEVICE']
        Query.__init__(self, columns, 10 * len(columns), 1)
        self.operator = 'write'


class UserBehaviorWorkload:
    def __init__(self, wq=0, q1=0, q2=0, q3=0):
        self.write_query = wq
        self.__write_query = wq
        self.q1 = q1
        self.__q1 = q1
        self.q2 = q2
        self.__q2 = q2
        self.q3 = q3
        self.__q3 = q3


    def pop(self) -> Query:
        if self.write_query > 0:
            result = UserBehaviorWriteQuery()
            result.cost = result.cost * self.write_query
            self.write_query = 0
            return result
        elif self.q1 > 0:
            result = UserBehaviorQ1()
            result.cost = result.cost * self.q1
            self.q1 = 0
            return result
        elif self.q2 > 0:
            result = UserBehaviorQ2()
            result.cost = result.cost * self.q2
            self.q2 = 0
            return result
        elif self.q3 > 0:
            result = UserBehaviorQ3()
            result.cost = result.cost * self.q3
            self.q3 = 0
            return result
        raise Exception("empty workload")

    def __len__(self):
        return self.write_query + self.q2 + self.q3 + self.q1

    def reset(self):
        self.write_query = self.__write_query
        self.q2 = self.__q2
        self.q3 = self.__q3
        self.q1 = self.__q1


