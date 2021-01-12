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
df = data_spark.limit(5)
df_pd = df.toPandas()

#%%
df_new = (
    df
    .withColumn('devices', f.explode_outer('devices'))
    .withColumn('tracks', f.explode_outer('tracks'))
)


df_new.show()
df_new_pd = df_new.toPandas()


#%%
def flatten_columns(df):

    def flattened_columns(df_schema: StructType, mode='raw',  prefix=''):
        column_names = list()

        for column in df_schema.fields:
            if isinstance(column.dataType, StructType):
                if mode == 'raw':
                    new_prefix = prefix + column.name + '.'
                else:
                    new_prefix = prefix + column.name + '_'

                column_names += [prefix + column_name
                                 for column_name
                                 in flattened_columns(column.dataType,
                                                      mode=mode,
                                                      prefix=new_prefix)
                ]

            else:
                column_names.append(prefix + column.name)

        return column_names

    columns_raw = flattened_columns(df.schema, mode='raw')
    columns_flattened = flattened_columns(df.schema, mode='flattened')

    return df.select(*[f.col(raw).alias(flattened)
                       for raw, flattened
                       in zip(columns_raw, columns_flattened)])

#%%


#%% Get the open
with open(data_files[0], 'rb') as file:
    avro_reader = fastavro.reader(file)

    for idx_in_file, observation in enumerate(avro_reader):
        print(observation)

        if idx_in_file >= 5:
            break


#%% F