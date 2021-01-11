#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan  8 19:09:49 2021

@author: s001284
"""

#%% Imports
# from toolbox.imports import *# Create a test dataframe
df = spark.createDataFrame(*t.test_dataframe())

df.printSchema()
df.show()


import fastavro
import numpy as np
import pandas as pd
import pyspark
import toolbox as t


from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import ArrayType, BooleanType, FloatType, IntegerType, \
    StringType, StructType, StructField


# Initialize a spark session
spark = (SparkSession.builder
         .appName('Wojciech_test')
         .getOrCreate())

# Create a test dataframe
df = spark.createDataFrame(*t.test_dataframe())

df.printSchema()
df.show()

#%% Load one data file

data_root = Path('/data/work/src/musicactivity/')
data_folders = [item for item in data_root.iterdir() if item.is_dir()]

folder_path = data_folders[0]
folder_name = folder_path.stem

files_in_folder = [item for item in folder_path.iterdir() if item.is_file()]
data_file = files_in_folder[0]
data_file_name = data_file.stem


#%% Load data
data = dict()
idx_observation = 0

with open(data_file, 'rb') as file:
    avro_reader = fastavro.reader(file)
    
    for idx_in_file, observation in enumerate(avro_reader):
        
        for attribute_observation in observation.keys():
            
df.printSchema()
df.show()
    # if an attribute is missing, add it to the data dict
    # and fill all the previous entries with null values.
    if attribute_observation not in data.keys():
        data[attribute_observation] = \
            [None for _ in range(idx_observation)]
    
    # As default, set all the attributes to null values.
    for attribute_data in data.keys():
        data[attribute_data].append(None)
    
    # Assign the values from the file to data dict.
    data[attribute_observation][idx_observation] = \
        observation[attribute_observation]
        
    idx_observation += 1

    
    
