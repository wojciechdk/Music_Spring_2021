#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan  8 19:09:49 2021

@author: s001284

for file in data_files:

"""

#%% Imports
from pyspark.shell import sc

from toolbox.config import Config
from toolbox.imports import *


#%% Initialize a Spark session
spark = t.spark.create_session('Music_Activity')


#%% Get the paths of data files and folders
data_root, data_subfolders, data_files = t.get_data_paths()


#%% Load data from files
df_raw = t.load_data_from_files(data_root, spark)


#%% Format the dataframe and its data
df = t.format_dataframe(df_raw)

number_of_records = 3571278161


#%% Create a sample dataframe

start_time = time.time()

n_samples = 1e5

df_sample = df.sample(fraction=n_samples / number_of_records,
                      withReplacement=False)

print(f'Execution time: {time.time() - start_time:.5f} s.')

#%% Save as pickle
start_time = time.time()

df_sample.rdd.saveAsPickleFile(str(Config.Path.project_data_root /
                                   'df_sample.pickle'))

print(f'Execution time: {time.time() - start_time:.5f} s.')


#%% Load from pickle
start_time = time.time()

rdd_from_pickle = sc.pickleFile('resources/data/df_sample.pickle').collect()

# rdd_from_pickle = sc.pickleFile(str(Config.Path.project_data_root /
#                                     'df_sample.pickle')).collect()

df_from_pickle = spark.createDataFrame(rdd_from_pickle)
df_from_pickle_pd = df_from_pickle.toPandas()

print(f'Execution time: {time.time() - start_time:.5f} s.')



#%% Count the number of records
start_time = time.time()

number_of_records = df.count()
print(f'Number of records: {number_of_records}.')

print(f'Execution time: {time.time() - start_time:.5f}')




#%% Get the open
with open(data_files[0], 'rb') as file:
    avro_reader = fastavro.reader(file)

    for idx_in_file, observation in enumerate(avro_reader):
        print(observation)

        if idx_in_file >= 5:
            break


#%%
start_time = time.time()

### CODE HERE

print(f'Execution time: {time.time() - start_time:.5f}')
