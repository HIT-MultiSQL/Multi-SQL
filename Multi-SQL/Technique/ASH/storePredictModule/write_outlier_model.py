from sklearn.linear_model import LogisticRegression
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler
import xgboost as xgb
from matplotlib import pyplot as plt
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os
import random


params = {
    "cs": {'path': "D:\\expdata\\cs-oltp\\", 'extra_layer': True},
    "rs": {'path': "D:\\expdata\\rs-oltp\\", 'extra_layer': True},
    "lsm": {'path': "D:\\expdata\\lsm-oltp\\", 'extra_layer': False}
}

# TODO this model is not well-tuned yet, and the result isn't suitable for all dataEngine
weight_model_bak = xgb.XGBRegressor(max_depth=5,
                         learning_rate=0.1,
                         n_estimators=100,
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
weight_model = xgb.XGBRegressor(max_depth=3,
                         learning_rate=0.1,
                         n_estimators=50,
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
scaler = StandardScaler()
outlier_model = LogisticRegression(penalty='l2', tol=0.1, solver='saga')
outlier_feature = ['tableSize', 'currentRows', 'writeRows', 'rowSize', 'intField', 'stringField', 'htg_updateRatio']
weight_feature = ['tableSize', 'currentRows', 'htg_updateRatioAvg']


# 从文件列表获取DataFrame
def read_data(file_name_list, param):
    path = param['path']
    size_list = []
    data = []
    first = True
    for filename in file_name_list:
        print("add %s to dataset" % filename)
        if first:
            data = pd.read_csv(path + filename)
            data = process_data(data, param)
            size_list.append(len(data))
            first = False
        else:
            tmp = pd.read_csv(path + filename)
            tmp = process_data(tmp, param)
            data = data.append(tmp)
            size_list.append(len(tmp))
    data = data.reset_index()
    return data, size_list


def process_data(data, param):
    # find outliers, first find all outliers
    data['outlier'] = 0
    writeRowIQR = data[data.writeRows > 0].quantile(0.75)['timePerRow'] -\
                  data[data.writeRows > 0].quantile(0.25)['timePerRow']
    writeRowMid = data[data.writeRows > 0].median()['timePerRow']
    for i in range(len(data)):
        if data['timePerRow'].iat[i] > writeRowMid+1.5*writeRowIQR:
            data['outlier'].iat[i] = 1

    # calculate accumulate outlier time
    data['full_outlier_time'] = 0
    if data['outlier'].iat[0] == 1:
        data['full_outlier_time'].iat[0] = data['timePerRow'].iat[0] * data['writeRows'].iat[0]
    outliers = 0
    for i in range(1, len(data)):
        if data['outlier'].iat[i] == 1:
            data['full_outlier_time'].iat[i] = data['full_outlier_time'].iat[i - 1] + data['timePerRow'].iat[i] * data['writeRows'].iat[i]
            outliers += 1
        else:
            data['full_outlier_time'].iat[i] = data['full_outlier_time'].iat[i - 1]
    total_outlier_time = data['full_outlier_time'].iat[len(data) - 1]
    print("full outliers: %d/%d, time:%.2f" % (outliers, len(data), total_outlier_time))

    if param['extra_layer']:
        # some outliers belongs to system overload, remove them
        writeRowIQR = data[(data.writeRows > 0) & (data.outlier == 1)].quantile(0.75)['timePerRow'] - \
                      data[(data.writeRows > 0) & (data.outlier == 1)].quantile(0.25)['timePerRow']
        writeRowMid = data[(data.writeRows > 0) & (data.outlier == 1)].median()['timePerRow']
        for i in range(len(data)):
            if data['outlier'].iat[i] == 1 and (data['timePerRow'].iat[i] > writeRowMid + 10 * writeRowIQR or data['timePerRow'].iat[i] > 500):
                data['outlier'].iat[i] = 0
        # calculate accumulate outlier time (overload point removed)
        data['part_outlier_time'] = 0
        if data['outlier'].iat[0] == 1:
            data['part_outlier_time'].iat[0] = data['timePerRow'].iat[0] * data['writeRows'].iat[0]
        outliers = 0
        for i in range(1, len(data)):
            if data['outlier'].iat[i] == 1:
                data['part_outlier_time'].iat[i] = data['part_outlier_time'].iat[i - 1] + data['timePerRow'].iat[i] * data['writeRows'].iat[i]
                outliers += 1
            else:
                data['part_outlier_time'].iat[i] = data['part_outlier_time'].iat[i - 1]
        total_outlier_time = data['part_outlier_time'].iat[len(data) - 1]
        print("part outliers: %d, time:%.2f" % (outliers, total_outlier_time))
    return data


def predict(train_set, test_set, test_length):
    # train outlier classifier
    outlier_X = train_set.filter(outlier_feature).copy()
    outlier_X = scaler.fit_transform(outlier_X)
    outlier_y = train_set['outlier']
    outlier_model.fit(outlier_X, outlier_y)

    weight_X = train_set[train_set.outlier == 1].filter(weight_feature).copy()
    weight_y = train_set[train_set.outlier == 1]['timePerRow'].copy()
    weight_X = weight_X.reset_index(drop=True)
    weight_y = weight_y.reset_index()
    weight_model.fit(weight_X, weight_y['timePerRow'])

    # predict outlier
    outlier_test_X = test_set.filter(outlier_feature).copy()
    outlier_test_X = scaler.transform(outlier_test_X)
    outlier_pred = outlier_model.predict_proba(outlier_test_X)
    tmp = []
    for pair in outlier_pred:
        tmp.append(pair[1])
    outlier_pred = tmp

    # predict weight
    weight_test_X = test_set.filter(weight_feature).copy()
    weight_pred = weight_model.predict(weight_test_X)

    # weight_pred = []
    # for i in range(len(test_set)):
    #     weight_pred.append(test_set['timePerRow'].iat[i])

    for i in range(len(weight_pred)):
        if weight_pred[i] < 0:
            weight_pred[i] = 0
    time_pred = []
    for i in range(len(test_length) - 1):
        time_pred.append(outlier_pred[test_length[i]] * weight_pred[test_length[i]] * test_set['writeRows'].iat[test_length[i]])
        for j in range(test_length[i] + 1, test_length[i + 1]):
            time_pred.append(time_pred[j - 1] + outlier_pred[j] * weight_pred[j] * test_set['writeRows'].iat[j])

    totalActualOut = 0
    totalPredictOut = 0
    for i in range(len(test_length) - 1):
        part = test_set.loc[test_length[i]:test_length[i+1]]
        actualOut = len(part[part.outlier == 1])
        predictOut = 0
        totalActualOut += actualOut
        for j in range(test_length[i], test_length[i+1]):
            predictOut += outlier_pred[j]
        totalPredictOut += predictOut
        print("%d, actual out:%d, predict out:%.2f" % (i, actualOut, predictOut))
    print("total actual out:%d, total predict out:%.2f" % (totalActualOut, totalPredictOut))
    return time_pred, weight_pred


# 绘制图像，包括测试集的线性拟合图像，测试集的预测拟合图像，测试集的真实图像
def graph(test_set, time_pred, outlier_pred, test_length, param):
    cols = 4
    factor = cols / 2
    rows = int((len(test_length) - 1) // factor)
    if rows < 2:
        rows = 2
    fig, ax = plt.subplots(nrows=rows, ncols=cols, figsize=(cols * 4, 3 * rows))

    for i in range(len(test_length) - 1):
        plotRow = int(i // factor)
        plotCol = int(i % factor * factor)
        x = np.arange(test_length[i + 1] - test_length[i])
        x1 = np.arange(test_length[i], test_length[i + 1])
        part = test_set.loc[test_length[i]:test_length[i+1]]
        out = part[part.outlier == 1]
        valid = part[part.outlier == 0]
        ax[plotRow][plotCol + 0].scatter(x=out.index, y=out['timePerRow'], s=3, c='red')
        ax[plotRow][plotCol + 0].scatter(x=valid.index, y=valid['timePerRow'], s=1, c='green')
        ax[plotRow][plotCol + 0].scatter(x=x1, y=outlier_pred[test_length[i]:test_length[i+1]], s=1, c='blue')
        ax[plotRow][plotCol + 1].scatter(x=x, y=test_set['full_outlier_time'].iloc[test_length[i]:test_length[i+1]],
                                         s=1, c='red')
        if param['extra_layer']:
            ax[plotRow][plotCol + 1].scatter(x=x, y=test_set['part_outlier_time'].iloc[test_length[i]:test_length[i + 1]],
                                             s=1, c='green')
        ax[plotRow][plotCol + 1].scatter(x=x, y=time_pred[test_length[i]:test_length[i+1]], s=1, c='blue')
    plt.show()


def pred_unknown():
    pred_outlier_X = np.zeros(shape=(3000000, 7))
    pred_outlier_X = pd.DataFrame(data=pred_outlier_X, columns=outlier_feature)
    pred_weight_X = np.zeros(shape=(3000000, 3))
    pred_weight_X = pd.DataFrame(data=pred_weight_X, columns=weight_feature)
    for i in range(3000000):
        pred_outlier_X['currentRows'].iat[i] = i * 4
        pred_weight_X['currentRows'].iat[i] = i * 4
    pred_outlier_X['tableSize'] = pred_outlier_X['currentRows'] * 121
    pred_weight_X['tableSize'] = pred_weight_X['currentRows'] * 121
    pred_outlier_X['writeRows'] = 4
    pred_outlier_X['intField'] = 4
    pred_outlier_X['rowSize'] = 121
    pred_outlier_X['stringField'] = 12
    pred_outlier_X['htg_updateRatio'] = 0.01
    pred_weight_X['htg_updateRatioAvg'] = 0.01

    pred_outlier_X = scaler.transform(pred_outlier_X)
    outlier_pred = outlier_model.predict_proba(pred_outlier_X)
    tmp = []
    for pair in outlier_pred:
        tmp.append(pair[1])
    outlier_pred = tmp
    weight_pred = weight_model.predict(pred_weight_X)
    time_pred = []
    time_pred.append(outlier_pred[0] * weight_pred[0])
    for j in range(1, 3000000):
        time_pred.append(time_pred[j - 1] + outlier_pred[j] * weight_pred[j])
    print("seq insert:%.2fvm" % time_pred[-1])

    pred_outlier_X = np.zeros(shape=(3000000, 7))
    pred_outlier_X = pd.DataFrame(data=pred_outlier_X, columns=outlier_feature)
    pred_weight_X = np.zeros(shape=(3000000, 3))
    pred_weight_X = pd.DataFrame(data=pred_weight_X, columns=weight_feature)
    for i in range(3000000):
        pred_outlier_X['currentRows'].iat[i] = i * 4
        pred_weight_X['currentRows'].iat[i] = i * 4
    pred_outlier_X['tableSize'] = pred_outlier_X['currentRows'] * 121
    pred_weight_X['tableSize'] = pred_weight_X['currentRows'] * 121
    pred_outlier_X['writeRows'] = 4
    pred_outlier_X['intField'] = 4
    pred_outlier_X['rowSize'] = 121
    pred_outlier_X['stringField'] = 12
    pred_outlier_X['htg_updateRatio'] = 0.66
    pred_weight_X['htg_updateRatioAvg'] = 0.66

    pred_outlier_X = scaler.transform(pred_outlier_X)
    outlier_pred = outlier_model.predict_proba(pred_outlier_X)
    tmp = []
    for pair in outlier_pred:
        tmp.append(pair[1])
    outlier_pred = tmp
    weight_pred = weight_model.predict(pred_weight_X)
    time_pred = []
    time_pred.append(outlier_pred[0] * weight_pred[0])
    for j in range(1, 3000000):
        time_pred.append(time_pred[j - 1] + outlier_pred[j] * weight_pred[j])
    print("rand insert:%.2fvm" % time_pred[-1])


param = params['lsm']
path = param['path']
all_data = os.listdir(param['path'])
train_set = []
test_set = []
for i in range(len(all_data)):
    if all_data[i].find("write.csv") != -1:
        if all_data[i].find("100") != -1:
            test_set.append(all_data[i])
        else:
            train_set.append(all_data[i])
train_set, a = read_data(train_set, param)
test_set, b = read_data(test_set, param)
length = [0]
for i in b:
    length.append(i + length[-1])
time_pred, outlier_pred = predict(train_set, test_set, length)

pred_unknown()
graph(test_set, time_pred, outlier_pred, length, param)


