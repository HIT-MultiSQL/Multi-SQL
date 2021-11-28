import numpy as np


def addonehot(rdata):
    rdata = np.zeros(rdata.shape)
    rdata = rdata[:, :, np.newaxis]
    res = np.zeros(rdata.shape)
    for _ in range(5):
        res = np.concatenate((rdata, res), axis=2)
    rdata = res[:, :, :, np.newaxis]
    res = np.zeros(rdata.shape)
    for _ in range(9):
        res = np.concatenate((rdata, res), axis=3)
    return res


def newonehot(x):
    res = np.zeros([10])
    charlist = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
    res[charlist.index(x)] = 1
    return res


def fillonehot(res, rdata):
    for i in range(30000):
        num = rdata[0][i]
        string = str(int(num))
        for ch in range(len(string)):
            res[0][i][ch] = newonehot(string[ch])
    return res


def get_onehot(rdata):
    res = addonehot(rdata)
    res = fillonehot(res, rdata)
    return res


if __name__ == '__main__':
    rdata = np.loadtxt('res.txt')
    np.savetxt('input.txt', rdata)