import xgboost as xgb
from sklearn import preprocessing
import sklearn.cluster as skc
import sklearn.metrics as metrics
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os

from sklearn.model_selection import GridSearchCV


class ReadModel:
    def __init__(self, name, retrain=False):
        params = {
            "rs_seq": {
                "name": "rs_seq",
                "drop_feature": ['latest10Writes', 'latest30Writes', 'latest60Writes', 'deltaCachePage'],
                "train_file_path": "../storePredictModule/rs-oltp",
                "filter": lambda x: x % 2 == 1,
                "max_depth": 6, "learning_rate": 0.1, "n_estimators": 200, "eps": 0.1},
            "rs_rand": {
                "name": "rs_rand",
                "drop_feature": ['latest10Writes', 'latest30Writes', 'latest60Writes', 'deltaCachePage'],
                "train_file_path": "../storePredictModule/rs-oltp",
                "filter": lambda x: x % 2 == 0,
                "max_depth": 6, "learning_rate": 0.1, "n_estimators": 200, "eps": 0.2},
            "lsm_seq": {
                "name": "lsm_seq",
                "drop_feature": ['latest10Writes', 'latest30Writes', 'latest60Writes', 'deltaCachePage',
                                 'log_totalReadRows'],
                "train_file_path": "../storePredictModule/lsm-oltp",
                "filter": lambda x: x % 2 == 1,
                "max_depth": 6, "learning_rate": 0.1, "n_estimators": 200, "eps": 0.1},
            "lsm_rand": {
                "name": "lsm_rand",
                "drop_feature": ['latest10Writes', 'latest30Writes', 'latest60Writes', 'deltaCachePage',
                                 'log_totalReadRows'],
                "train_file_path": "../storePredictModule/lsm-oltp",
                "filter": lambda x: x % 2 == 0,
                "max_depth": 6, "learning_rate": 0.1, "n_estimators": 200, "eps": 0.1},
        }
        self.__target = params[name]
        self.__useDBScan = True
        self.__retrain = retrain
        if not os.path.isfile(self.__target['name'] + "_read_model.json"):
            self.__retrain = True
        # 加载样本数据集
        file_list = os.listdir(self.__target["train_file_path"])
        train_file_list = []
        test_file_list = []
        for filename in file_list:
            if filename.find("read") != -1:
                begin = filename.find("test")
                end = filename.find(".")
                num = int(filename[begin + 4:end])
                if num >= 51 and self.__target['filter'](num):
                    test_file_list.append(filename)
                elif num < 51 and self.__target['filter'](num):
                    train_file_list.append(filename)
        if __name__ == '__main__':
            print("训练集数据数:%d , 测试集数据数:%d" % (len(train_file_list), len(test_file_list)))
        self.__model = xgb.XGBRegressor(
            max_depth=self.__target['max_depth'],
            learning_rate=self.__target['learning_rate'],
            n_estimators=self.__target['n_estimators'],
            silent=True,
            objective='reg:linear',
            nthread=-1,
            gamma=0,
            min_child_weight=3,
            max_delta_step=0,
            subsample=0.8,
            colsample_bytree=0.7,
            colsample_bylevel=1,
            reg_alpha=0,
            reg_lambda=1,
            seed=1440,
            missing=None)
        if self.__retrain:
            self.__train(train_file_list)
            self.__model.save_model(self.__target['name'] + "_read_model.json")
        else:
            self.__model.load_model(self.__target['name'] + "_read_model.json")
        # load data for use
        self.__data_test, self.__test_size = self.__read_csv(self.__target["train_file_path"] + "/", test_file_list)
        self.__X_test = self.__data_test.drop(columns=['timePerRow'])
        self.__y_test = self.__data_test['timePerRow']
        self.__y_pred = None

    def __dbscan(self, data):
        feature = data.filter(items=['readRows', 'timePerRow'])
        feature = preprocessing.scale(feature)
        db = skc.DBSCAN(eps=self.__target['eps'], min_samples=3).fit(feature)
        labels = db.labels_
        data['cluster'] = labels
        data = data[data.cluster != -1]
        data = data.drop(columns=['cluster'])
        data = data.reset_index()
        return data

    def __extra_process(self, data):
        pass
        return data

    def __pre_process(self, data):
        if data['storeModel'].iat[0] == "LSM":
            data['isLSM'] = 1
            data['isBT'] = 0
        elif data['storeModel'].iat[0] == "BT":
            data['isLSM'] = 0
            data['isBT'] = 1
        else:
            data['isLSM'] = 0
            data['isBT'] = 0
        columns = ['storeModel', 'timeStampMill', 'batchID', 'useMicroTimeAvg', 'useTimeMicro', 'keyVariance']
        columns.extend(self.__target['drop_feature'])
        data = data.drop(columns=columns)
        if self.__useDBScan:
            data = self.__dbscan(data)
        return data

    def __read_csv(self, path, file_name_list, need_extra_process=False):
        size_list = []
        data = 0
        first = True
        for filename in file_name_list:
            if __name__ == '__main__':
                print("add %s to dataset" % filename)
            if first:
                data = pd.read_csv(path + filename)
                data = self.__pre_process(data)
                if need_extra_process:
                    data = self.__extra_process(data)
                data.reset_index()
                size_list.append(len(data))
                first = False
            else:
                tmp = pd.read_csv(path + filename)
                tmp = self.__pre_process(tmp)
                if need_extra_process:
                    tmp = self.__extra_process(tmp)
                data = data.append(tmp, ignore_index=True, verify_integrity=True)
                data.reset_index()
                size_list.append(len(data))
            if np.isnan(data['timePerRow'].iat[len(data) - 1]):
                raise Exception("data corrupted")
        return data, size_list

    def __train(self, train_file_list):
        data_train, self.__train_size = self.__read_csv(self.__target["train_file_path"] + "/", train_file_list)
        self.__X_train = data_train.drop(columns=['timePerRow'])
        print(self.__X_train.columns)
        self.__y_train = data_train['timePerRow']
        self.__model.fit(self.__X_train, self.__y_train, eval_metric='rmse')
        return [self.__X_train, self.__y_train]

    def test(self):
        y_pred = self.__model.predict(self.__X_test)
        self.__y_pred = pd.Series(data=y_pred, index=self.__y_test.index, name='timePerRow')
        for i in range(len(self.__y_pred)):
            if self.__y_pred.iat[i] < 0:
                self.__y_pred.iat[i] = 0

    def show_graph(self, plot_importance=False):
        if (not self.__retrain) and self.__y_pred is None:
            print("no graph to plot")
            return
        # use different colors to draw graph
        fig, ax = plt.subplots(nrows=2, ncols=2, figsize=(16, 12))
        ax1 = ax[0][0]
        ax2 = ax[0][1]
        ax3 = ax[1][0]
        ax4 = ax[1][1]
        # ax1: test data validate
        if self.__y_pred is not None:
            ax1.scatter(x=np.arange(len(self.__y_pred)), y=self.__y_pred, s=1, c='mediumblue', label='predict')
            ax1.scatter(x=np.arange(len(self.__y_test)), y=self.__y_test, s=1, c='g', label='actual')
            ax1.vlines(self.__test_size, 0, 1, transform=ax1.get_xaxis_transform(), colors='r')
            y_pred_avg = []
            y_test_avg = []
            for i in range(len(self.__test_size)):
                y_pred_avg.append(0)
                y_test_avg.append(0)
                startPos = 0
                if i > 0:
                    startPos = self.__test_size[i - 1]
                for j in range(startPos, self.__test_size[i]):
                    y_pred_avg[i] += self.__y_pred.iat[j]
                    y_test_avg[i] += self.__y_test.iat[j]
                y_pred_avg[i] = y_pred_avg[i] / (self.__test_size[i] - startPos)
                y_test_avg[i] = y_test_avg[i] / (self.__test_size[i] - startPos)
                if i == 0:
                    ax1.hlines(y_pred_avg[i], startPos, self.__test_size[i], colors='b', label='predictAvg')
                    ax1.hlines(y_test_avg[i], startPos, self.__test_size[i], colors='lime', label='actualAvg')
                else:
                    ax1.hlines(y_pred_avg[i], startPos, self.__test_size[i], colors='b')
                    ax1.hlines(y_test_avg[i], startPos, self.__test_size[i], colors='lime')
            ax1.set_ylabel('TimePerRow(micro seconds)')
            # ax1.set_xlabel('Data ID')
            ax1.legend()
            print(y_pred_avg)
            print(y_test_avg)

        # ax2: train data result
        if self.__retrain:
            y_train_p = self.__model.predict(self.__X_train)
            y_train_p = pd.Series(data=y_train_p, index=self.__y_train.index, name='timePerRow')
            ax2.scatter(x=np.arange(len(y_train_p)), y=y_train_p, s=1, c='mediumblue', label='predict')
            ax2.scatter(x=np.arange(len(self.__y_train)), y=self.__y_train, s=1, c='g', label='actual')
            ax2.vlines(self.__train_size, 0, 1, transform=ax2.get_xaxis_transform(), colors='r')
            me2 = metrics.mean_absolute_error(self.__y_train, y_train_p)
            print("mean_absolute_error for train: %.2f" % (me2))

        # ax3: test data validate by row
        data_test = self.__data_test.sort_values(by='readRows')
        data_pred = self.__X_test.copy()
        data_pred['timePerRow'] = self.__y_pred
        ax3.scatter(x=data_test['readRows'], y=data_test['timePerRow'], s=1, c='g', label='actual')
        ax3.scatter(x=data_pred['readRows'], y=data_pred['timePerRow'], s=1, c='b', label='predict')
        ax3.set_title(self.__target['name'])
        ax3.set_xlabel('ReadRows')
        ax3.set_ylabel('TimePerRow(micro seconds)')
        ax3.legend()

        # 计算准确率
        if self.__y_pred is not None and __name__ == '__main__':
            me1 = metrics.mean_absolute_error(self.__y_test, self.__y_pred)
            print("mean_absolute_error for validate: %.2f" % (me1))
            if plot_importance:
                xgb.plot_importance(self.__model)
            plt.show()

    def predict(self, row_size=121, read_rows=1, current_rows=11997996, is_random=0, key_field: int = 4,
                value_field: int = 12):
        for i in range(len(self.__test_size)):
            X_want = self.__X_test.head(sum(self.__test_size[:i + 1]))
            X_want = X_want.tail(self.__test_size[i])
            X_want = X_want.head(10).copy()
            if 'rowSize' in X_want.columns:
                X_want['rowSize'] = row_size
            if 'readRows' in X_want.columns:
                X_want['readRows'] = read_rows
            if 'tableSize' in X_want.columns:
                X_want['tableSize'] = row_size * current_rows
            if 'currentRows' in X_want.columns:
                X_want['currentRows'] = current_rows
            if 'isRandom' in X_want.columns:
                X_want['isRandom'] = is_random
            if 'intField' in X_want.columns:
                X_want['intField'] = key_field
            if 'stringField' in X_want.columns:
                X_want['stringField'] = value_field
            y_want = self.__model.predict(X_want)
            return sum(y_want) / len(y_want)

    def search(self):
        file_list = os.listdir(self.__target["train_file_path"])
        train_file_list = []
        test_file_list = []
        for filename in file_list:
            if filename.find("read") != -1:
                begin = filename.find("test")
                end = filename.find(".")
                num = int(filename[begin + 4:end])
                if num >= 51 and self.__target['filter'](num):
                    test_file_list.append(filename)
                elif num < 51 and self.__target['filter'](num):
                    train_file_list.append(filename)
        self.__train(train_file_list)
        X_train = self.__X_train
        y_train = self.__y_train
        cv_params = {'n_estimators': [100, 200, 300, 400, 500]}
        other_params = {'learning_rate': 0.1, 'n_estimators': 500, 'max_depth': 6, 'min_child_weight': 3, 'seed': 1440,
                        'subsample': 0.8, 'colsample_bytree': 0.7, 'gamma': 0, 'reg_alpha': 0, 'reg_lambda': 1,
                        'objective': 'reg:linear', 'gamma': 0, 'max_child_step': 0, 'colsample_bylevel': 1, 'missing': None}
        model = xgb.XGBRegressor(**other_params)
        print("开始了！！！")
        optimized_GBM = GridSearchCV(estimator=model, param_grid=cv_params, scoring='r2', cv=5, verbose=1, n_jobs=4)
        optimized_GBM.fit(X_train, y_train)
        print("最好的参数模型：\n", optimized_GBM.best_estimator_)
        print('参数的最佳取值：{0}'.format(optimized_GBM.best_params_))
        print('最佳模型得分:{0}'.format(optimized_GBM.best_score_))

if __name__ == '__main__':
    model = ReadModel("rs_rand", retrain=False)
    # model.search()
    model.test()
    # model.show_graph(True)
    result = model.predict(row_size=39, read_rows=1, current_rows=10000, is_random=0, key_field=1, value_field=5)
    print(result)
    # model.show_graph(True)
    result = model.predict(row_size=46, read_rows=1, current_rows=100000, is_random=0, key_field=1, value_field=4)
    print(result)
    model.show_graph(True)

