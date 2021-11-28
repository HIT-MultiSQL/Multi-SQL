import sys
import random
from tensorflow import keras
from tensorflow.keras import layers
import numpy as np
import onehot
import json_reader as js
import re
import os
import data_load
import mongo


def machine_index(data_path, workload_path, doc_name, field, path, database, collection):
    # load data
    data_list = js.load_blog_data(data_path)
    
    res, origin_map = js.get_frequently_field(data_list, field, 0)
    
    # load workload
    res = js.load_blog_workload(res, origin_map, workload_path, doc_name, field)
    col = data_load.mongoConnect(database, collection)

    x_test = onehot.get_onehot(res)

    print(x_test)

    def divide(result):
        for member in result:
            if isinstance(member, list):
                divide(member)
            else:
                if member > 0.5:
                    result[result.index(member)] = 1
                else:
                    result[result.index(member)] = 0

    def convolution():
        inn = layers.Input(shape=(sequence_length, alpha_len, embedding_dimension, 1))
        cnns = []
        for i, size in enumerate(filter_sizes):
            conv = layers.Conv3D(filters=2, kernel_size=([size, alpha_len, embedding_dimension]),
                                 strides=[size, 1, 1], padding='valid', activation='relu')(inn)
            # if i%2:
            pool_size = int(conv.shape[1] / 100)
            pool = layers.MaxPool3D(pool_size=([pool_size, 1, 1]), padding='valid')(conv)
            # pool = MaxMin(pool_size)(conv)

            cnns.append(pool)
        outt = layers.concatenate(cnns)

        model = keras.Model(inputs=inn, outputs=outt, name='cnns')
        model.summary()
        return model

    filter_sizes = [5, 10, 20, 30, 50]
    embedding_dimension = 10
    sequence_length = 30000
    alpha_len = 6

    model = keras.Sequential([
        layers.Input(shape=([sequence_length, alpha_len, embedding_dimension])),
        layers.Reshape((sequence_length, alpha_len, embedding_dimension, 1)),
        convolution(),
        layers.Flatten(),
        layers.Dropout(0.1),
        layers.Dense(64, activation='relu'),
        layers.Dropout(0.1),
        layers.Dense(4, activation='sigmoid')
    ])

    model.compile(optimizer=keras.optimizers.Adam(),
                  loss=keras.losses.BinaryCrossentropy(),
                  metrics=['accuracy'])
    model.summary()

    #x_test = np.load("input.npy")

    model.load_weights(path+"/2/weight")

    y_hat = model.predict(x_test)
    y_hat = y_hat.tolist()
    divide(y_hat)

    def index_string(fv):
        if fv[0] == 0:
            return 'no index'
        if fv[3] == 1:
            if fv[1] == 1:
                return 'sparse b-tree index'
            elif fv[2] == 1:
                return 'partial b-tree index'
            else:
                return 'b-tree index'
        else:
            if fv[1] == 1:
                return 'sparse hash index'
            elif fv[2] == 1:
                return 'partial hash index'
            else:
                return 'hash index'

    select_index = index_string(y_hat[0])

    rate = mongo.testout(col, res, origin_map, field, y_hat[0])

    res = field + '\n\t\t' + select_index
    return res, rate


if __name__ == '__main__':
    
    my_args = []
    for i in range(1, len(sys.argv)): 
        my_args.append(sys.argv[i])
    y_hat ,rate= machine_index(my_args[0],my_args[1],my_args[2],my_args[3],my_args[4],my_args[5],my_args[6])
    print('block')
    print(y_hat)
    print('block')
    print(rate)
