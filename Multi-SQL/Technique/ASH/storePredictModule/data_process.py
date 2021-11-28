# -*- coding: utf-8 -*-
"""
Created on Mon Mar  2 17:11:24 2020

@author: iron
"""

import os
import pandas as pd
'''
for rs,lsm log data, use log_to_csv to generate csv file, use processCsv to split file and calculate necessary column
for col log data, use col_log_to_csv to generate csv file, use processCsv to split file and calculate necessary column
'''


def log_to_csv(dir_name="D:\\expdata\\rawdata"):
    file_list = os.listdir(dir_name)
    for fileName in file_list:
        if fileName.endswith(".log") and fileName.find("test") != -1:
            infile = open(dir_name + "/" + fileName, "r")
            outfile = open(dir_name + "/" + fileName[:-3] + "csv", "w")
            store_model = infile.readline().strip()
            metas = infile.readline().strip().split(", ")
            value_range = metas[0].split(":")[1]
            consecutive = metas[1].split(":")[1]
            row_size = metas[3].split(":")[1]
            key_field = metas[4].split(":")[1]
            value_field = metas[5].split(":")[1]
            
            head = "storeModel,valueRange,consecutive,rowSize,intField,stringField,"+infile.readline()
            outfile.write(head)
            for oldLine in infile.readlines():
                append_data = store_model + "," + value_range + "," + consecutive + "," + row_size + "," + key_field + \
                     "," + value_field + ","
                outfile.write(append_data + oldLine)
                
            infile.close()
            outfile.close()
            print(fileName + " OK")


def col_log_to_csv(dir_name="D:\\expdata\\collog"):
    file_list = os.listdir(dir_name)
    for fileName in file_list:
        if fileName.endswith(".log"):
            infile = open(dir_name + "/" + fileName, "r")
            outfile = open(dir_name + "/" + fileName[:-3] + "csv", "w")
            store_model = infile.readline().strip()
            metas = infile.readline().strip().split(", ")
            value_range = metas[0].split(":")[1]
            consecutive = metas[1].split(":")[1]
            row_size = metas[3].split(":")[1]
            int_field = metas[4].split(":")[1]
            string_field = metas[5].split(":")[1]
            
            head = "storeModel,valueRange,consecutive,rowSize,intField,stringField,"+infile.readline()
            outfile.write(head)
            for oldLine in infile.readlines():
                append_data = store_model + "," + value_range + "," + consecutive + "," + row_size + "," + int_field + \
                     "," + string_field + ","
                outfile.write(append_data + oldLine)
            infile.close()
            outfile.close()
            print(fileName + " OK")


def process_csv(dir_name="D:\\expdata\\csvdata"):
    file_list = os.listdir(dir_name)
    for filename in file_list:
        if filename.find('test') == -1:
            continue
        l_pos = filename.find('test')+4
        r_pos = filename.find('.')
        is_random = 0
        seq_no = int(filename[l_pos:r_pos])
        if seq_no % 2 == 0:
            is_random = 1
        data = pd.read_csv(dir_name + "/" + filename)
        # 获取单行性能
        data['timePerRow'] = 0.0
        data['isRandom'] = is_random 
        for i in range(len(data)):
            row = max(data['readRows'][i], data['writeRows'][i])
            if row == 0:
                row = 1
            data.at[i, 'timePerRow'] = data['useTimeMicro'][i]/row
        read_data_size = len(data[data.readRows > 0])
        write_data_size = len(data[data.writeRows > 0])
        if read_data_size > 0:
            # 拆分出读记录
            read_data = data[(data.readRows > 0)]
            read_data.to_csv(dir_name+filename[:-3]+"read.csv", index=False)
        if write_data_size > 0:
            # 拆分出写记录
            write_data = data[(data.writeRows > 0)]
            write_data.to_csv(dir_name+filename[:-3]+"write.csv", index=False)
        print(filename, " done")


process_csv('cs-csv')
