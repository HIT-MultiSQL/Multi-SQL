import xgboost as xgb
from xgboost import plot_importance
from sklearn import preprocessing
import sklearn.cluster as skc
import sklearn.metrics as metrics
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os

drop_feature=['latest10Writes','latest30Writes','latest60Writes','currentRows','deltaCachePage']
reTrain = True

def split_outlier(data, mode):
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
    print("total use time:", data['useTimeMicro'].sum())
    columns.extend(drop_feature)
    data = data.drop(columns=columns)
    data['outlier'] = 0


    if mode == 'w':
        writeRowIQR = data[data.writeRows > 0].quantile(0.75)['timePerRow'] - data[data.writeRows > 0].quantile(0.25)[
            'timePerRow']
        writeRowMid = data[data.writeRows > 0].median()['timePerRow']
        for i in range(len(data)):
            if data['timePerRow'].iat[i] > writeRowMid+3*writeRowIQR:
                data['outlier'].iat[i] = 1
    elif mode == 'r':
        X = data.filter(items=['readRows', 'timePerRow'])
        X = preprocessing.scale(X)
        db = skc.DBSCAN(eps=0.2, min_samples=3).fit(X)
        labels = db.labels_
        data['outlier'] = labels
        for i in range(len(data)):
            if data['outlier'].iat[i] == -1:
                data['outlier'].iat[i] = 1
            else:
                data['outlier'].iat[i] = 0
    return data


def drawGraph(data, mode):
    grain = 200
    out = data[data.outlier==1]
    valid = data[data.outlier==0]
    data['outlier_time'] = 0
    data['total_time'] = 0
    if mode == 'w':
        if data['outlier'].iat[0] == 1:
            data['outlier_time'].iat[0] = data['timePerRow'].iat[0] * data['writeRows'].iat[0]
        data['total_time'].iat[0] = data['timePerRow'].iat[0] * data['writeRows'].iat[0]
    elif mode == 'r':
        if data['outlier'].iat[0] == 1:
            data['outlier_time'].iat[0] = data['timePerRow'].iat[0] * data['readRows'].iat[0]
        data['total_time'].iat[0] = data['timePerRow'].iat[0] * data['readRows'].iat[0]
    for i in range(1, len(data)):
        if mode == 'w':
            if data['outlier'].iat[i] == 1:
                data['outlier_time'].iat[i] = data['outlier_time'].iat[i - 1] + data['timePerRow'].iat[i] * data['writeRows'].iat[i]
            else:
                data['outlier_time'].iat[i] = data['outlier_time'].iat[i - 1]
        elif mode == 'r':
            if data['outlier'].iat[i] == 1:
                data['outlier_time'].iat[i] = data['outlier_time'].iat[i - 1] + data['timePerRow'].iat[i] * data['readRows'].iat[i]
            else:
                data['outlier_time'].iat[i] = data['outlier_time'].iat[i - 1]
    for i in range(1, len(data)):
        if mode == 'w':
            data['total_time'].iat[i] = data['total_time'].iat[i - 1] + data['timePerRow'].iat[i] * data['writeRows'].iat[i]
        elif mode == 'r':
            data['total_time'].iat[i] = data['total_time'].iat[i - 1] + data['timePerRow'].iat[i] * data['readRows'].iat[i]

    bar = np.zeros(int(data['timePerRow'].max() / grain) + 1)
    foo = np.zeros(int(data['timePerRow'].max() / grain) + 1)
    for i in range(len(data)):
        if data['outlier'].iat[i] == 1:
            if mode == 'w':
                bar[int(data['timePerRow'].iat[i] / grain)] += data['writeRows'].iat[i]
            else:
                bar[int(data['timePerRow'].iat[i] / grain)] += data['readRows'].iat[i]
            foo[int(data['timePerRow'].iat[i] / grain)] += 1

    fig, ax = plt.subplots(nrows=1, ncols=4, figsize=(16, 3))
    ax[0].scatter(x=out.index, y=out['timePerRow'], s=1, c='red')
    ax[0].scatter(x=valid.index, y=valid['timePerRow'], s=1, c='green')
    ax[1].scatter(x=np.arange(len(data)), y=data['outlier_time'], s=1, c='red')
    ax[1].scatter(x=np.arange(len(data)), y=data['total_time'], s=1, c='green')
    ax[2].bar(np.arange(0, len(bar)), bar)
    ax[3].bar(np.arange(0, len(foo)), foo)
    data = data.reset_index()


    return data

# 加载样本数据集
fileName ="D:\\expdata\\cs-oltp\\ctest1001.write.csv"

# 观察离群点
data = pd.read_csv(fileName)
data = split_outlier(data, 'w')
drawGraph(data, 'w')
plt.show()