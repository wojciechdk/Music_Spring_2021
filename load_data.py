#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan  8 19:09:49 2021

@author: s001284

for file in data_files:

"""

#%% Imports
from toolbox.imports import *

#%% Initialize a spark session
spark = (SparkSession.builder
         .appName('Wojciech_test')
         .getOrCreate())

#%%
def time_it(function):
    def wrapper():
        start_time = time.time()
        output = function()
        execution_time = time.time() - start_time
        print(f'Execution time: {execution_time:.5f} s.')
        return output

    return wrapper

#%% Get the paths of data files and folders
data_root = Path('/data/work/src/musicactivity/')
data_folders = [item for item in data_root.iterdir() if item.is_dir()]
data_files = [item 
              for folder in data_folders
              for item in folder.iterdir()
              if item.is_file() and item.suffix == '.avro']

print(f'Number of folders: {len(data_folders)}')
print(f'Number of files: {len(data_files)}')


#%%
@time_it
def read_pandas_1():
    return t.avro_to_pandas(data_files[0])


@time_it
def read_pandas_2():
    return pandavro.read_avro(str(data_files[0]))


@time_it
def read_spark():
    return spark.read.format('avro').load(str(data_files[0]))


data_pandas_1 = read_pandas_1()
data_pandas_2 = read_pandas_2()
data_spark = read_spark()


#%%
df = data_spark.head(5)





#%% Get the open
with open(data_files[0], 'rb') as file:
    avro_reader = fastavro.reader(file)

    for idx_in_file, observation in enumerate(avro_reader):
        print(observation)

        if idx_in_file >= 5:
            break


#%% F