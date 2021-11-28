import numpy as np
from workload import Workload, LineItemMeta
from sklearn.cluster import AgglomerativeClustering


class ColGroupRecommend:
    def __init__(self, table, decay_factor=1):
        self.table = table
        self.decay_factor = decay_factor

    # 进行对相应columns的一个编码，字符形式
    def __encode_columns(self, columns):
        encode = []
        for i in range(len(self.table.columns)):
            encode.append('0')
        for column in columns:
            encode[self.table.get_column_no(column)] = '1'
        return "".join(encode)

    # 进行对相应columns的一个编码，int形式
    def __get_feature_vector(self, columns):
        result = np.zeros(len(self.table.columns))
        for column in columns:
            result[self.table.get_column_no(column)] = 1
        return result

    @staticmethod
    def __pop_with_threshold(freq_set, threshold):
        mark = []
        for item in freq_set.keys():
            if freq_set[item] < threshold:
                mark.append(item)
        for item in mark:
            freq_set.pop(item)
        return freq_set

    @staticmethod
    def __distance(x1, x2):
        return np.linalg.norm(x1 - x2)

    def recommend(self, workload, n_cluster=3):
        freq_set = {}
        while len(workload) != 0:
            q = workload.pop()
            encoded_feature = self.__encode_columns(q.columns)
            if encoded_feature in freq_set:
                freq_set[encoded_feature] += q.cost
            else:
                freq_set[encoded_feature] = q.cost
        # 去除非频繁项。非频繁项阈值：100*len(Columns), 即小于100行的总查询结果。剩余频繁项中取前20个查询
        threshold = 100 * len(self.table.columns)
        freq_set = self.__pop_with_threshold(freq_set, threshold)
        if len(freq_set) > 20:
            costs = list(freq_set.values())
            costs.sort(reverse=True)
            threshold = costs[19]
            freq_set = self.__pop_with_threshold(freq_set, threshold)

        vectors = []
        vector_labels = list(freq_set.keys())
        for i in range(len(self.table.columns)):
            vectors.append(np.zeros(shape=(len(freq_set))))
        for i in range(len(vector_labels)):
            for j in range(len(self.table.columns)):
                if vector_labels[i][j] == '1':
                    vectors[j][i] += freq_set[vector_labels[i]] / len(vector_labels[i])
        clustering = AgglomerativeClustering(n_clusters=n_cluster).fit(vectors)
        ret = []
        for i in range(n_cluster):
            result = []
            for j in range(len(clustering.labels_)):
                if clustering.labels_[j] == i:
                    result.append(self.table.columns[j])
            ret.append(result)
        return ret

    def recommend_tile(self, workload, n_cluster):
        represent = []
        represent_weight = []
        while len(workload) != 0:
            q = workload.pop()
            current_feature = self.__get_feature_vector(q.columns)
            weight = q.cost
            if len(represent) < n_cluster:
                find = False
                for i in range(len(represent)):
                    if np.array_equiv(represent[i], current_feature):
                        find = True
                        represent_weight[i] += weight
                if not find:
                    represent.append(current_feature)
                    represent_weight.append(weight)
            else:
                min_pos = -1
                min_dis = float("inf")
                for i in range(n_cluster):
                    dis = self.__distance(current_feature, represent[i])
                    if dis < min_dis:
                        min_dis = dis
                        min_pos = i
                represent[min_pos] = (represent[min_pos] * represent_weight[min_pos] * self.decay_factor) + \
                    current_feature * weight
                represent_weight[min_pos] = represent_weight[min_pos] * self.decay_factor + weight
                represent[min_pos] = represent[min_pos] / represent_weight[min_pos]

        for i in range(len(represent)):
            represent[i] = [represent[i], represent_weight[i]]
        represent.sort(key=lambda a: a[1], reverse=True)

        rest = []
        ret = []
        for i in range(len(self.table.columns)):
            rest.append(1)
        for r in represent:
            result = []
            for i in range(len(r[0])):
                if r[0][i] > 0.5 and rest[i] == 1:
                    rest[i] = 0
                    result.append(self.table.columns[i])
            if len(result) > 0:
                ret.append(result)
        need_print = False
        for i in rest:
            if i == 1:
                need_print = True
        if need_print:
            result = []
            for i in range(len(rest)):
                if rest[i] == 1:
                    result.append(self.table.columns[i])
            ret.append(result)
        return ret


if __name__ == '__main__':
    # tile_group_recommend(3, generateQueryLog())
    # column_groups_recommend(3, generateQueryLog())
    rec = ColGroupRecommend(LineItemMeta())
    w1 = Workload(100, 100, 100, 5, 5)
    print(rec.recommend_tile(w1, 3))
