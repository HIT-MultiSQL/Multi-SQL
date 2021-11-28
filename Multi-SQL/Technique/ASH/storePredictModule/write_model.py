import xgboost as xgb
import sklearn.metrics as metrics
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os
from typing import List


class WriteModel:
    def __init__(self, name, retrain=False):
        params = {
            "cs": {
                "name": "cs",
                "drop_feature": ['latest10Writes','latest30Writes','latest60Writes','currentRows','deltaCachePage'],
                "train_file_path": "../storePredictModule/cs-oltp",
                "max_depth": 6, "learning_rate": 0.2, "n_estimators": 100},
            "rs": {
                "name": "rs",
                "drop_feature": ['latest10Writes','latest30Writes','latest60Writes','currentRows','deltaCachePage'],
                "train_file_path": "../storePredictModule/rs-oltp",
                "max_depth": 8, "learning_rate": 0.1, "n_estimators": 500},
            "lsm": {
                "name": "lsm",
                "drop_feature": ['latest10Writes','latest60Writes'],
                "train_file_path": "../storePredictModule/lsm-oltp",
                "max_depth": 8, "learning_rate": 0.2, "n_estimators": 600},
        }
        self.__target = params[name]
        self.__retrain= retrain
        if not os.path.isfile(self.__target['name']+"_write_model.json"):
            self.__retrain = True
        # 加载样本数据集
        fileList = os.listdir(self.__target['train_file_path'])
        train_file_list = []
        test_file_list = []
        for filename in fileList:
            if filename.find("write") != -1:
                begin = filename.find("test")
                end = filename.find(".")
                num = int(filename[begin + 4:end])
                if num >= 51:
                    test_file_list.append(filename)
                elif num < 51:
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
            self.__model.save_model(self.__target['name'] + "_write_model.json")
        else:
            self.__model.load_model(self.__target['name'] + "_write_model.json")
        # load data for use
        self.__data_test, self.__test_size = self.__read_csv(self.__target['train_file_path'] + "/", test_file_list, True)
        self.__X_test = self.__data_test.drop(columns=['timePerRow'])
        self.__y_test = self.__data_test['timePerRow']
        self.__y_pred = None

    def __pre_process(self, data, is_test):
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

        writeRowIQR = data[data.writeRows > 0].quantile(0.75)['timePerRow'] - data[data.writeRows > 0].quantile(0.25)[
            'timePerRow']
        writeRowMid = data[data.writeRows > 0].median()['timePerRow']

        if is_test:
            out = data[(data.timePerRow > writeRowMid + 1.5 * writeRowIQR) | (
                    data.timePerRow < writeRowMid - 1.5 * writeRowIQR)].copy()
            out = out.reset_index()
            outTotalRow = out['writeRows'].sum()
            outUseTime = 0
            for i in range(len(out)):
                outUseTime = outUseTime + out['writeRows'].iat[i] * out['timePerRow'].iat[i]
            if __name__ == '__main__':
                print("outlier time:", outUseTime)
                print("outlier row:", outTotalRow)
                print("total row:", data['writeRows'].sum())
        # TODO shorten outlier range
        data = data[(data.writeRows > 0) & (data.timePerRow < writeRowMid + 1.5 * writeRowIQR) & (
                data.timePerRow > writeRowMid - 1.5 * writeRowIQR)]
        data = data.reset_index()
        return data

    def __read_csv(self, path, file_name_list, is_test):
        size_list = []
        data = 0
        first = True
        for filename in file_name_list:
            if __name__ == '__main__':
                print("add %s to dataset" % filename)
            if first:
                data = pd.read_csv(path + filename)
                data = self.__pre_process(data, is_test)
                size_list.append(len(data))
                first = False
            else:
                tmp = pd.read_csv(path + filename)
                tmp = self.__pre_process(tmp, is_test)
                data = data.append(tmp)
                size_list.append(len(data))
        return data, size_list

    def __train(self, train_file_list):
        data_train, self.__train_size = self.__read_csv(self.__target['train_file_path'] + "/", train_file_list, False)
        self.__X_train = data_train.drop(columns=['timePerRow'])
        self.__y_train = data_train['timePerRow']
        self.__model.fit(self.__X_train, self.__y_train, eval_metric='rmse')

    def test(self):
        y_pred = self.__model.predict(self.__X_test)
        self.__y_pred = pd.Series(data=y_pred, index=self.__y_test.index, name='timePerRow')
        for i in range(len(self.__y_pred)):
            if self.__y_pred.iat[i] < 0:
                self.__y_pred.iat[i] = 0
        self.__y_pred_avg = []
        self.__y_test_avg = []
        for i in range(len(self.__test_size)):
            self.__y_pred_avg.append(0)
            self.__y_test_avg.append(0)
            startPos = 0
            if i > 0:
                startPos = self.__test_size[i - 1]
            for j in range(startPos, self.__test_size[i]):
                self.__y_pred_avg[i] += self.__y_pred.iat[j]
                self.__y_test_avg[i] += self.__y_test.iat[j]
            self.__y_pred_avg[i] = self.__y_pred_avg[i] / (self.__test_size[i] - startPos)
            self.__y_test_avg[i] = self.__y_test_avg[i] / (self.__test_size[i] - startPos)


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
            for i in range(len(self.__test_size)):
                startPos = 0
                if i > 0:
                    startPos = self.__test_size[i - 1]
                if i == 0:
                    ax1.hlines(self.__y_pred_avg[i], startPos, self.__test_size[i], colors='b', label='predictAvg')
                    ax1.hlines(self.__y_test_avg[i], startPos, self.__test_size[i], colors='lime', label='actualAvg')
                else:
                    ax1.hlines(self.__y_pred_avg[i], startPos, self.__test_size[i], colors='b')
                    ax1.hlines(self.__y_test_avg[i], startPos, self.__test_size[i], colors='lime')
            ax1.set_ylabel('TimePerRow(micro seconds)')
            ax1.set_xlabel('Data ID')
            ax1.legend()

        # ax2: train data result
        if self.__retrain:
            y_train_p = self.__model.predict(self.__X_train)
            y_train_p = pd.Series(data=y_train_p, index=self.__y_train.index, name='timePerRow')
            ax2.scatter(x=np.arange(len(y_train_p)), y=y_train_p, s=1, c='mediumblue', label='predict')
            ax2.scatter(x=np.arange(len(self.__y_train)), y=self.__y_train, s=1, c='g', label='actual')
            ax2.vlines(self.__train_size, 0, 1, transform=ax2.get_xaxis_transform(), colors='r')
            me2 = metrics.mean_absolute_error(self.__y_train, y_train_p)
            print("mean_absolute_error for train: %.2f" % (me2))
            y_train_avg = []
            y_train_p_avg = []
            for i in range(len(self.__train_size)):
                y_train_avg.append(0)
                y_train_p_avg.append(0)
                startPos = 0
                if i > 0:
                    startPos = self.__train_size[i - 1]
                for j in range(startPos, self.__train_size[i]):
                    y_train_avg[i] += self.__y_train.iat[j]
                    y_train_p_avg[i] += y_train_p.iat[j]
                y_train_avg[i] = y_train_avg[i] / (self.__train_size[i] - startPos)
                y_train_p_avg[i] = y_train_p_avg[i] / (self.__train_size[i] - startPos)
                ax2.hlines(y_train_avg[i], startPos, self.__train_size[i], colors='lime')
                ax2.hlines(y_train_p_avg[i], startPos, self.__train_size[i], colors='b')

        # ax3, ax4: test data validate by row
        data_test_1 = self.__data_test.head(self.__test_size[0])
        data_test_2 = self.__data_test.tail(self.__test_size[-1] - self.__test_size[-2])
        data_test_1 = data_test_1.sort_values(by='writeRows')
        data_test_2 = data_test_2.sort_values(by='writeRows')
        data_pred = self.__X_test.copy()
        data_pred['timePerRow'] = self.__y_pred
        data_pred_1 = data_pred.head(self.__test_size[0])
        data_pred_2 = data_pred.tail(self.__test_size[-1] - self.__test_size[-2])
        data_pred_1 = data_pred_1.sort_values(by='writeRows')
        data_pred_2 = data_pred_2.sort_values(by='writeRows')
        ax3.scatter(x=data_test_1['writeRows'], y=data_test_1['timePerRow'], s=1, c='g', label='actual')
        ax3.scatter(x=data_pred_1['writeRows'], y=data_pred_1['timePerRow'], s=1, c='b', label='predict')
        ax3.set_title('serial')
        ax3.set_xlabel('WriteRows')
        ax3.set_ylabel('TimePerRow(micro seconds)')
        ax3.legend()
        ax4.scatter(x=data_test_2['writeRows'], y=data_test_2['timePerRow'], s=1, c='g', label='actual')
        ax4.scatter(x=data_pred_2['writeRows'], y=data_pred_2['timePerRow'], s=1, c='b', label='predict')
        ax4.set_title('random')
        ax4.set_xlabel('WriteRows')
        ax4.set_ylabel('TimePerRow(micro seconds)')
        ax4.legend()

        # 计算准确率
        if self.__y_pred is not None and __name__ == '__main__':
            me1 = metrics.mean_absolute_error(self.__y_test, self.__y_pred)
            print("mean_absolute_error for validate: %.2f" % (me1))
            if plot_importance:
                xgb.plot_importance(self.__model)
            plt.show()

    def predict(self, row_size: int, key_field: int, value_field: int, total_rows: int, engine: str) -> List[float]:
        X_test = self.__X_test.copy()
        row_scale = total_rows / 11997996
        if engine == 'wt':
            size_scale = total_rows / 11997996 * row_size / 121
        else:
            size_scale = total_rows / 11997996 * row_size / 138
        for i in range(len(X_test)):
            if 'currentRows' in X_test.columns:
                X_test['currentRows'].iat[i] = X_test['currentRows'].iat[i] * row_scale
            if 'intField' in X_test.columns:
                X_test['intField'].iat[i] = key_field
            if 'stringField' in X_test.columns:
                X_test['stringField'].iat[i] = value_field
            if 'rowSize' in X_test.columns:
                X_test['rowSize'].iat[i] = row_size
            if 'tableSize' in X_test.columns:
                X_test['tableSize'].iat[i] = X_test['tableSize'].iat[i] * size_scale
        y_pred = self.__model.predict(X_test)
        y_pred = pd.Series(data=y_pred, index=self.__y_test.index, name='timePerRow')
        for i in range(len(y_pred)):
            if y_pred.iat[i] < 0:
                y_pred.iat[i] = 0
        y_pred_avg = []
        for i in range(len(self.__test_size)):
            y_pred_avg.append(0)
            startPos = 0
            if i > 0:
                startPos = self.__test_size[i - 1]
            for j in range(startPos, self.__test_size[i]):
                y_pred_avg[i] += y_pred.iat[j]
            y_pred_avg[i] = y_pred_avg[i] / (self.__test_size[i] - startPos)
        return y_pred_avg


if __name__ == '__main__':
    model = WriteModel("lsm")
    print(model.predict(39, 1, 5, 10000, 'rocks'))
    model.show_graph()

