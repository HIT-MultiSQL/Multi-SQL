from typing import List


class LineItemMeta:
    def __init__(self):
        self.columns = ['ORDERKEY', 'PARTKEY', 'SUPPKEY', 'LINENUMBER', 'QUANTITY', 'EXTENDEDPRICE', 'DISCOUNT', 'TAX',
                        'RETURNFLAG', 'LINESTATUS', 'SHIPDATE', 'COMMITDATE', 'RECEIPTDATE', 'SHIPINSTRUCT', 'SHIPMODE',
                        'COMMENT']
        self.keys = ['ORDERKEY', 'PARTKEY', 'SUPPKEY', 'LINENUMBER']
        self.field_len = {"wt": [4, 4, 4, 2, 2, 9, 4, 4, 1, 1, 10, 10, 10, 17, 7, 43],
                          "rocks": [8, 6, 5, 1, 2, 9, 4, 4, 1, 1, 10, 10, 10, 17, 7, 43]}
        self.key_len = {"wt": 14, "rocks": 20}

    def get_row_length(self, column_no_list: List[int or str], engine: str) -> int:
        ret = 0
        for no in column_no_list:
            if type(no) is int:
                ret += self.field_len[engine][no]
            elif type(no) is str:
                ret += self.field_len[engine][self.get_column_no(no)]
        return ret

    def get_column_no(self, column_name: str) -> int:
        # fix typo in another project
        if column_name == 'EXTENDPRICE':
            column_name = 'EXTENDEDPRICE'
        for i in range(len(self.columns)):
            if self.columns[i] == column_name:
                return i
        raise Exception("column name " + column_name + " not found")


class Query:
    def __init__(self, columns, cost, affect_rows):
        self.columns = columns
        self.cost = cost
        self.affect_rows = affect_rows


class Q3(Query):
    def __init__(self):
        columns = ['ORDERKEY', 'EXTENDEDPRICE', 'DISCOUNT', 'SHIPDATE']
        Query.__init__(self, columns, 300000 * len(columns), 300000)


class Q5(Query):
    def __init__(self):
        columns = ['ORDERKEY', 'SUPPKEY', 'EXTENDEDPRICE', 'DISCOUNT']
        Query.__init__(self, columns, 90000 * len(columns), 90000)


class ReadQuery1(Query):
    def __init__(self):
        columns = ['ORDERKEY', 'PARTKEY', 'SUPPKEY', 'LINENUMBER', 'QUANTITY', 'EXTENDEDPRICE', 'DISCOUNT', 'TAX',
                   'RETURNFLAG', 'LINESTATUS', 'SHIPDATE', 'COMMITDATE', 'RECEIPTDATE', 'SHIPINSTRUCT', 'SHIPMODE',
                   'COMMENT']
        Query.__init__(self, columns, 10 * len(columns), 1)


class ReadQuery2(Query):
    def __init__(self):
        columns = ['ORDERKEY', 'PARTKEY', 'SUPPKEY', 'LINENUMBER', 'QUANTITY', 'EXTENDEDPRICE', 'DISCOUNT',
                   'RETURNFLAG', 'LINESTATUS', 'SHIPDATE', 'COMMITDATE', 'SHIPMODE']
        Query.__init__(self, columns, 10 * len(columns), 1)


class WriteQuery(Query):
    def __init__(self):
        columns = ['ORDERKEY', 'PARTKEY', 'SUPPKEY', 'LINENUMBER', 'QUANTITY', 'EXTENDEDPRICE', 'DISCOUNT', 'TAX',
                   'RETURNFLAG', 'LINESTATUS', 'SHIPDATE', 'COMMITDATE', 'RECEIPTDATE', 'SHIPINSTRUCT', 'SHIPMODE',
                   'COMMENT']
        Query.__init__(self, columns, len(columns), 1)
        self.operator = 'write'


class Workload:
    def __init__(self, wq=0, rq1=0, rq2=0, q3=0, q5=0):
        self.write_query = wq
        self.__write_query = wq
        self.read_query_1 = rq1
        self.__read_query_1 = rq1
        self.read_query_2 = rq2
        self.__read_query_2 = rq2
        self.q3 = q3
        self.__q3 = q3
        self.q5 = q5
        self.__q5 = q5

    def pop(self) -> Query:
        if self.write_query > 0:
            result = WriteQuery()
            result.cost = result.cost * self.write_query
            self.write_query = 0
            return result
        elif self.read_query_1 > 0:
            result = ReadQuery1()
            result.cost = result.cost * self.read_query_1
            self.read_query_1 = 0
            return result
        elif self.read_query_2 > 0:
            result = ReadQuery2()
            result.cost = result.cost * self.read_query_2
            self.read_query_2 = 0
            return result
        elif self.q3 > 0:
            result = Q3()
            result.cost = result.cost * self.q3
            self.q3 = 0
            return result
        elif self.q5 > 0:
            result = Q5()
            result.cost = result.cost * self.q5
            self.q5 = 0
            return result
        raise Exception("empty workload")

    def __len__(self):
        return self.write_query + self.read_query_1 + self.read_query_2 + self.q3 + self.q5

    def reset(self):
        self.write_query = self.__write_query
        self.read_query_1 = self.__read_query_1
        self.read_query_2 = self.__read_query_2
        self.q3 = self.__q3
        self.q5 = self.__q5
