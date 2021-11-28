import re


def find_sub_query(sql, cypher):
    # 将WHERE前后分开
    result_where = re.match(".*WHERE *(.*)", sql)
    # 查找pname
    pname_pattern = re.compile("pname")
    pname_result = pname_pattern.findall(sql)
    query_length = len(pname_result)  # 语句数
    number_list = []  # 建立标号表
    var_list = []  # 建立常量/变量表
    asc_number = 0
    for i in range(query_length):
        number_list.append([asc_number, asc_number + 1])
        var_list.append([False, False])
        asc_number += 2
    # 初始化标号表
    initial_pattern = re.compile("([a-z])\\.([a-z])name *= *([a-z])\\.([a-z])name")
    initial_result = initial_pattern.findall(sql)
    initial_dict = {"s": 0, "o": 1}
    if initial_result:
        for result_tuple in initial_result:
            first_part = number_list[ord(result_tuple[0]) - 97][initial_dict[result_tuple[1]]]
            second_part = number_list[ord(result_tuple[2]) - 97][initial_dict[result_tuple[3]]]
            number_list[ord(result_tuple[0]) - 97][initial_dict[result_tuple[1]]] = min(first_part, second_part)
            number_list[ord(result_tuple[2]) - 97][initial_dict[result_tuple[3]]] = min(first_part, second_part)

    # 初始化常量变量表
    var_pattern = re.compile("([a-z])\\.([so])name *= *'[^']*'")
    var_result = var_pattern.findall(sql)
    if var_result:
        for result_tuple in var_result:
            var_list[ord(result_tuple[0]) - 97][initial_dict[result_tuple[1]]] = True

    # start to choose sub query p
    oname_community = []  # 存储sub query的编号
    oname_community_letters = []
    for number_count in range(len(number_list)):
        row_flag = 0
        for row_count in range(2):
            if var_list[number_count][row_count]:
                row_flag += 1
            else:
                if number_list[number_count][row_count] != 2 * number_count + row_count:  # 条件一：如果这个编号已经被修改了，那么说明有相同的值
                    row_flag += 1
                else:
                    community_flag = False
                    for j in range(len(number_list)):  # 条件二：如果这个编号是所有的相同值中最小值，那么有可能它没有变化
                        if (number_list[j][1] == number_list[number_count][row_count] or
                            number_list[j][0] == number_list[number_count][row_count]) \
                                and j != number_count:
                            community_flag = True
                    if community_flag:
                        row_flag += 1
        if row_flag == 2:  # add this p to new sub query
            oname_community.append(number_count)
            oname_community_letters.append(chr(number_count + 97))
    # 第三步是找到相应的p
    p_list = []
    for oname in oname_community_letters:
        result_p = re.search(oname + "\\.pname *= *'([^ ]*)'", result_where.group(1))
        p_list.append(result_p.group(1))
    # if don't have sub query,then stop and return False
    if len(oname_community) == 0:
        print("Don't have sub query")
        return False, None, None, None, None, None
    if len(oname_community) == query_length:
        # 全是子结构
        print("The whole query is a sub query")
        result_return = re.search("RETURN([^\n\r]*)", cypher)
        return_part = result_return.group(1)
        parts = return_part.split(',')
        for part in parts:
            return_part = re.sub(part.strip(), part.strip(), return_part)
        cypher = re.sub("RETURN([^\n\r]*)", "RETURN" + return_part, cypher)
        # p_cypher = re.sub(":subject", "", p_cypher)
        # p_cypher = re.sub(":object", "", p_cypher)
        pred_results = re.compile(":predicate{type:'([^']*)'").findall(cypher)
        for result in pred_results:
            cypher = re.sub(":predicate{type:'" + result + "'}", ":`" + result + "`", cypher)
        return True, p_list, sql, cypher, "", 0

    # 提取所有的语句编号
    pattern2 = re.compile('([a-z]).pname')
    result_all_word_letter = pattern2.findall(result_where.group(1))
    others_in_query = set(result_all_word_letter).difference(set(oname_community_letters))  # 非子查询的编号字母

    # add new part
    first_class = []
    second_class = []
    third_class = []

    # judge first\third class
    for oname in oname_community:
        s_flag = False
        o_flag = False
        for word in others_in_query:

            if number_list[ord(word) - 97][0] == number_list[oname][0] or \
                    number_list[ord(word) - 97][1] == number_list[oname][0]:
                third_class.append(number_list[oname][0])
                s_flag = True
            if number_list[ord(word) - 97][0] == number_list[oname][1] or \
                    number_list[ord(word) - 97][1] == number_list[oname][1]:
                third_class.append(number_list[oname][1])
                o_flag = True
        if not s_flag:
            first_class.append(number_list[oname][0])
        if not o_flag:
            first_class.append(number_list[oname][1])
    # judge second class
    for word in others_in_query:
        s_flag = False
        o_flag = False
        for oname in oname_community:
            if number_list[oname][0] == number_list[ord(word) - 97][0] or \
                    number_list[oname][1] == number_list[ord(word) - 97][0]:
                s_flag = True
            if number_list[oname][0] == number_list[ord(word) - 97][1] or \
                    number_list[oname][1] == number_list[ord(word) - 97][1]:
                o_flag = True
        if not s_flag:
            second_class.append(number_list[ord(word) - 97][0])
        if not o_flag:
            second_class.append(number_list[ord(word) - 97][1])
    first_class = list(set(first_class))
    second_class = list(set(second_class))
    third_class = list(set(third_class))
    third_class.sort()
    second_class.sort()
    first_class.sort()

    # judge select number list
    # 取出select部分
    select_part_result = re.search("SELECT (.*) FROM", sql)
    select_part = select_part_result.group()
    select_part_pattern = re.compile("([a-z])\\.([a-z])name")
    select_part_all_result = select_part_pattern.findall(select_part)  # SELECT 部分的字母
    select_number_list = []
    for value in select_part_all_result:
        if value[1] == 's':
            select_number_list.append(number_list[ord(value[0]) - 97][0])
        else:
            select_number_list.append(number_list[ord(value[0]) - 97][1])

    connection_point = []
    connection_point.extend(third_class)
    # add first to other select part
    first_select_number_list = []
    for value in first_class:
        if value in select_number_list:
            first_select_number_list.append(value)

    # sub mysql query select part
    sub_mysql_select_part = []
    sub_mysql_select_part.extend(connection_point)
    sub_mysql_select_part.extend(first_select_number_list)



    # # 根据原query的SELECT部分寻找衔接点
    # all_result_dict = dict()
    # connection_point = []  # 连接点的序号存储
    # all_result_ng = []
    # for select_part_all_result_value in select_part_all_result:
    #     result = ord(select_part_all_result_value[0]) - 97
    #     if result not in all_result_ng:
    #         all_result_dict[result] = select_part_all_result_value[1]
    #
    # for result in all_result_dict.keys():
    #     if all_result_dict[result] == 's':
    #         if not var_list[result][1]:
    #             connection_point.append(number_list[result][1])
    #     else:
    #         if not var_list[result][0]:
    #             connection_point.append(number_list[result][0])
    # connection_point = list(set(connection_point))
    # connection_point.sort()  # 从小到大排序

    # 7.1 清除原query的非子结构部分:FROM和WHERE部分

    sub_mysql = sql  # 新的子查询
    for word_letter in others_in_query:
        sub_mysql = re.sub(", *[a-zA-Z0-9]* AS " + str(word_letter), "", sub_mysql)
        sub_mysql = re.sub("FROM *[a-zA-Z0-9]* AS " + str(word_letter) + " *, *", "FROM ", sub_mysql)
        sub_mysql = re.sub("WHERE *" + str(word_letter) + "\\.pname *= *[^ ]* *AND", "WHERE ", sub_mysql)
        sub_mysql = re.sub("AND *" + str(word_letter) + "\\.pname *= *[^ ]*", "", sub_mysql)
        sub_mysql = re.sub("[AND]* *" + str(word_letter) + "\\.oname *= *([^= ]*)", "", sub_mysql)
        sub_mysql = re.sub("[AND]* *" + str(word_letter) + "\\.sname *= *([^= ]*)", "", sub_mysql)
    sub_mysql = re.sub(" *[AND]* [a-z]\\.[a-z]name *= *[a-z]\\.[a-z]name", "", sub_mysql)  # 除去所有连接语句
    sub_mysql = re.sub("WHERE *AND", "WHERE ", sub_mysql)

    # 创建新的连接语句
    already_build = set()  # 重复的连接句要去掉
    for number in oname_community:
        sname_number = number_list[number][0]
        oname_number = number_list[number][1]
        for i in range(len(number_list)):
            if i == number:
                continue
            if chr(i + 97) in others_in_query:
                continue
            if number_list[i][0] == sname_number and (number, 0, i, 0) not in already_build:
                sub_mysql += " AND " + str(chr(number + 97)) + ".sname=" + str(chr(i + 97)) + ".sname"
                already_build.add((number, 0, i, 0))
                already_build.add((i, 0, number, 0))
            elif number_list[i][1] == sname_number and (number, 0, i, 1) not in already_build:
                sub_mysql += " AND " + str(chr(number + 97)) + ".sname=" + str(chr(i + 97)) + ".oname"
                already_build.add((number, 0, i, 1))
                already_build.add((i, 1, number, 0))
            if number_list[i][0] == oname_number and (number, 1, i, 0) not in already_build:
                sub_mysql += " AND " + str(chr(number + 97)) + ".oname=" + str(chr(i + 97)) + ".sname"
                already_build.add((number, 1, i, 0))
                already_build.add((i, 0, number, 1))
            elif number_list[i][1] == oname_number and (number, 1, i, 1) not in already_build:
                sub_mysql += " AND " + str(chr(number + 97)) + ".oname=" + str(chr(i + 97)) + ".oname"
                already_build.add((number, 1, i, 1))
                already_build.add((i, 1, number, 1))

    # 设置子查询的SELECT部分：SELECT部分
    # 先构建SELECT的替换语句,其中连接点表已经从小到大排序
    select_exchange = []
    for point in sub_mysql_select_part:
        for i in range(len(number_list)):
            if i not in oname_community:
                continue
            if number_list[i][0] == point:
                select_exchange.append((i, 's'))
                break
            elif number_list[i][1] == point:
                select_exchange.append((i, 'o'))
                break
    # original_select_length = len(select_exchange)
    # for value in select_part_all_result:
    #     if value[0] in oname_community_letters:
    #         select_exchange.append((ord(value[0]) - 97, value[1]))

    select_string = ""
    for exchange in select_exchange:
        exchange_letter = chr(int(exchange[0]) + 97)
        exchange_name = str(exchange[1]) + 'name'
        select_string += str(exchange_letter) + "." + str(exchange_name) + ", "
    select_string = select_string.rstrip(", ")
    # 替换语句
    sub_mysql = re.sub("SELECT (.*) FROM", "SELECT " + select_string + " FROM", sub_mysql)

    # 处理Cypher语句
    # 从5的子结构中找出子查询的语句编号，选择出语句
    # 提取每一句语句：
    pattern = re.compile("\\(\\([^()]*\\)-\\[:predicate{type:'[^{}]*'}\\]->"
                         "\\([^()]*\\)\\)")
    result_match = pattern.findall(cypher)  # 提取到的每个MATCH语句的列表
    # 建立新语句
    sub_cypher = "MATCH "
    for oname_number in oname_community:
        sub_cypher += result_match[oname_number] + ", "
    sub_cypher = sub_cypher.rstrip(", ")
    # 根据6中的连接点序号添加RETURN部分，其中序号从小到大排序
    # 首先，找到序号对应的语句位置
    return_exchange = []  # 包括语句位置的元组
    for point in sub_mysql_select_part:
        for i in range(len(number_list)):
            if i not in oname_community:
                continue
            if number_list[i][0] == point:
                return_exchange.append((i, 'subject'))
                break
            elif number_list[i][1] == point:
                return_exchange.append((i, 'object'))
                break
    # for value in select_part_all_result:
    #     if value[0] in oname_community_letters:
    #         if value[1] == 's':
    #             return_exchange.append((ord(value[0]) - 97, 'subject'))
    #         else:
    #             return_exchange.append((ord(value[0]) - 97, 'object'))

    # 根据语句位置，获取变量名
    return_var = []  # 装变量名
    for exchange in set(return_exchange):
        var_name_word = result_match[exchange[0]]
        if exchange[1] == 'subject':
            result_var = re.search("\\(([^()]*):subject\\)", var_name_word)
        else:
            result_var = re.search("\\(([^()]*):object\\)", var_name_word)
        var_name = result_var.group(1)
        return_var.append(var_name)
    # 添加RETURN语句
    sub_cypher += " RETURN "
    for var in return_var:
        sub_cypher += str(var) + ".name, "
    sub_cypher = sub_cypher.rstrip(", ")
    # for query successfully, need to change something wrong
    # sub_cypher = re.sub(":subject", "", sub_cypher)
    # sub_cypher = re.sub(":object", "", sub_cypher)
    pred_results = re.compile(":predicate{type:'([^']*)'").findall(sub_cypher)
    for result in pred_results:
        sub_cypher = re.sub(":predicate{type:'" + result + "'}", ":`" + result + "`", sub_cypher)

    # 生成mysql非子查询部分，用于最终查询
    # 将所有子查询部分去除
    other_mysql = sql  # 新的子查询
    for word_letter in oname_community_letters:
        other_mysql = re.sub(", *[a-zA-Z0-9]* AS " + str(word_letter), "", other_mysql)
        other_mysql = re.sub("FROM *[a-zA-Z0-9]* AS " + str(word_letter) + " *, ", "FROM ", other_mysql)
        other_mysql = re.sub("AND " + str(word_letter) + "\\.[a-z]name *= *[a-z]\\.[a-z]name", "", other_mysql)
        other_mysql = re.sub("WHERE *" + str(word_letter) + "\\.[a-z]name *= *[a-z]\\.[a-z]name", "WHERE",
                             other_mysql)
        other_mysql = re.sub("AND *[a-z]\\.[a-z]name *= *" + str(word_letter) + "\\.[a-z]name", "", other_mysql)
        other_mysql = re.sub("WHERE *[a-z]\\.[a-z]name *= *" + str(word_letter) + "\\.[a-z]name", "WHERE",
                             other_mysql)
        other_mysql = re.sub("[AND]* *" + str(word_letter) + "\\.pname *= *([^= ]*)", "", other_mysql)
        other_mysql = re.sub("[AND]* *" + str(word_letter) + "\\.oname *= *([^= ]*)", "", other_mysql)
        other_mysql = re.sub("[AND]* *" + str(word_letter) + "\\.sname *= *([^= ]*)", "", other_mysql)
    other_mysql = re.sub("WHERE *AND", "WHERE ", other_mysql)
    other_mysql = re.sub(" *[AND]* [a-z]\\.[a-z]name *= *[a-z]\\.[a-z]name", "", other_mysql)  # remove the relation
    # 创建新的连接语句
    already_build = set()  # 重复的连接句要去掉
    for letter in others_in_query:
        number = ord(letter) - 97
        sname_number = number_list[number][0]
        oname_number = number_list[number][1]
        for i in range(len(number_list)):
            if i == number:
                continue
            if chr(i + 97) in oname_community_letters:
                continue
            if number_list[i][0] == sname_number and (number, 0, i, 0) not in already_build:
                other_mysql += " AND " + str(chr(number + 97)) + ".sname=" + str(chr(i + 97)) + ".sname"
                already_build.add((number, 0, i, 0))
                already_build.add((i, 0, number, 0))
            elif number_list[i][1] == sname_number and (number, 0, i, 1) not in already_build:
                other_mysql += " AND " + str(chr(number + 97)) + ".sname=" + str(chr(i + 97)) + ".oname"
                already_build.add((number, 0, i, 1))
                already_build.add((i, 1, number, 0))
            if number_list[i][0] == oname_number and (number, 1, i, 0) not in already_build:
                other_mysql += " AND " + str(chr(number + 97)) + ".oname=" + str(chr(i + 97)) + ".sname"
                already_build.add((number, 1, i, 0))
                already_build.add((i, 0, number, 1))
            elif number_list[i][1] == oname_number and (number, 1, i, 1) not in already_build:
                other_mysql += " AND " + str(chr(number + 97)) + ".oname=" + str(chr(i + 97)) + ".oname"
                already_build.add((number, 1, i, 1))
                already_build.add((i, 1, number, 1))

    other_select_result = re.search("SELECT .* FROM", other_mysql)
    other_select = other_select_result.group()
    for word in oname_community_letters:
        other_select = re.sub(word + "\\.sname *(as " + word + "sname)?,?", "", other_select)
        other_select = re.sub(word + "\\.oname *(as " + word + "oname)?,?", "", other_select)
    for number in first_select_number_list:
        for row_num in range(len(number_list)):
            for col_num in range(len(number_list[row_num])):
                if number_list[row_num][col_num] == number:
                    word = chr(row_num)
                    if col_num == 0:
                        other_select = re.sub(word + "\\.sname (as " + word + "sname)?,?", "", other_select)
                    else:
                        other_select = re.sub(word + "\\.oname (as " + word + "oname)?,?", "", other_select)

    other_mysql = re.sub("SELECT .* FROM", other_select, other_mysql)

    # 在后面填上所有和连接点相关的点的值，用in关键字
    in_values = []
    for point in connection_point:
        for i in range(len(number_list)):
            if i in oname_community:
                continue
            if number_list[i][0] == point:
                in_values.append((chr(i + 97), 's'))
                break
            elif number_list[i][1] == point:
                in_values.append((chr(i + 97), 'o'))
                break
    in_string = " AND "
    value_num = 0
    for value in in_values:
        in_string += value[0] + "." + value[1] + "name='{0[" + str(value_num) + "]}' AND "
        value_num += 1
    in_string = in_string.rstrip(' AND ')
    other_mysql += in_string

    sub_mysql = re.sub("\n", " ", sub_mysql)
    sub_mysql = re.sub(", *FROM", " FROM", sub_mysql)
    other_mysql = re.sub("\n", " ", other_mysql)
    other_mysql = re.sub(", *FROM", " FROM", other_mysql)
    other_mysql = re.sub(" +", " ", other_mysql)
    sub_mysql = re.sub(" +", " ", sub_mysql)
    return True, p_list, sub_mysql, sub_cypher, other_mysql, len(connection_point)


if __name__ == '__main__':
    print(find_sub_query(
        "SELECT a.sname FROM dbpedia2 AS a, dbpedia1 AS b WHERE a.pname='<http://dbpedia.org/property/birthPlace>' AND b.pname='<http://dbpedia.org/property/deathPlace>' AND a.sname=b.sname AND a.oname='<http://dbpedia.org/resource/London>' AND b.oname='<http://dbpedia.org/resource/London>' ",
        "MATCH ((subj:subject)-[:predicate{type:'<http://dbpedia.org/property/birthPlace>'}]->(:object{name:'<http://dbpedia.org/resource/London>'})),((subj:subject)-[:predicate{type:'<http://dbpedia.org/property/deathPlace>'}]->(:object{name:'<http://dbpedia.org/resource/London>'})) RETURN subj"
    ))
