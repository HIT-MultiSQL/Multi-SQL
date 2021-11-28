from typing import List
from workload import Query


class UserInformMeta:
    def __init__(self):
        self.columns = ['ID', 'NAME', 'AGE', 'GENDER', 'OCCUPATION', 'REGISTER_DATE']
        self.keys = ['ID']
        self.field_len = {"wt": [5, 6, 4, 1, 15, 8],
                          "rocks": [5, 6, 4, 1, 15, 8]}
        self.key_len = {"wt": 5, "rocks": 5}

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


class UserInformQ2(Query):
    def __init__(self):
        columns = ['ID', 'REGISTER_DATE']
        Query.__init__(self, columns, 400 * len(columns), 400)


class UserInformQ3(Query):
    def __init__(self):
        columns = ['ID', 'REGISTER_DATE']
        Query.__init__(self, columns, 1000 * len(columns), 1000)


class UserInformQ4(Query):
    def __init__(self):
        columns = ['ID', 'NAME', 'AGE', 'GENDER', 'OCCUPATION', 'REGISTER_DATE']
        Query.__init__(self, columns, 600 * len(columns), 600)


class UserInformQ5(Query):
    def __init__(self):
        columns = ['ID', 'AGE']
        Query.__init__(self, columns, 1500 * len(columns), 1500)


class UserInformWriteQuery(Query):
    def __init__(self):
        columns = ['ID', 'NAME', 'AGE', 'GENDER', 'OCCUPATION', 'REGISTER_DATE']
        Query.__init__(self, columns, len(columns) * 1.5, 1)
        self.operator = 'write'


class UserInformWorkload:
    def __init__(self, wq=0, q2=0, q3=0, q4=0, q5=0):
        self.write_query = wq
        self.__write_query = wq
        self.q2 = q2
        self.__q2 = q2
        self.q3 = q3
        self.__q3 = q3
        self.q4 = q4
        self.__q4 = q4
        self.q5 = q5
        self.__q5 = q5

    def pop(self) -> Query:
        if self.write_query > 0:
            result = UserInformWriteQuery()
            result.cost = result.cost * self.write_query
            self.write_query = 0
            return result
        elif self.q2 > 0:
            result = UserInformQ2()
            result.cost = result.cost * self.q2
            self.q2 = 0
            return result
        elif self.q3 > 0:
            result = UserInformQ3()
            result.cost = result.cost * self.q3
            self.q3 = 0
            return result
        elif self.q4 > 0:
            result = UserInformQ4()
            result.cost = result.cost * self.q4
            self.q4 = 0
            return result
        elif self.q5 > 0:
            result = UserInformQ5()
            result.cost = result.cost * self.q5
            self.q5 = 0
            return result
        raise Exception("empty workload")

    def __len__(self):
        return self.write_query + self.q2 + self.q3 + self.q4 + self.q5

    def reset(self):
        self.write_query = self.__write_query
        self.q2 = self.__q2
        self.q3 = self.__q3
        self.q4 = self.__q4
        self.q5 = self.__q5


