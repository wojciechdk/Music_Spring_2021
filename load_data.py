#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan  8 19:09:49 2021

@author: s001284
"""

#%% Imports
# from toolbox.imports import *

import fastavro
import numpy as np
import pandas as pd
import pyspark
import toolbox as t
import time

from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import ArrayType, BooleanType, FloatType, IntegerType, \
    NullType, StringType, StructType, StructField


#%% Get the paths of data files and folders
data_root = Path('/data/work/src/musicactivity/')
data_folders = [item for item in data_root.iterdir() if item.is_dir()]
     
data_files = [item 
              for folder in data_folders
              for item in folder.iterdir()
              if item.is_file() and item.suffix == '.avro']

print(f'Number of folders: {len(data_folders)}')
print(f'Number of files: {len(data_files)}')


#%% Get the open

with open(data_files[0], 'rb') as file:
    avro_reader = fastavro.reader(file)

    for idx_in_file, observation in enumerate(avro_reader):
        print(observation)

        if idx_in_file >= 5:
            break

#%% Find all attributes:
folder_path = data_folders[0]  
files_in_folder = [item for item in folder_path.iterdir() if item.is_file()]
data_file = files_in_folder[0]
data_file_name = data_file.stem


data = dict()
idx_observation = 0
all_attributes = list()

start_time = time.time()

for data_file in files_in_folder:
    
    with open(data_file, 'rb') as file:
        avro_reader = fastavro.reader(file)
        
        for idx_in_file, observation in enumerate(avro_reader):
            
            for attribute_observation in observation.keys():
                    
                # if an attribute is missing, add it to the data dict
                # and fill all the previous entries with null values.
                if attribute_observation not in all_attributes:
                    all_attributes.append(attribute_observation)
                
execution_time = time.time() - start_time
print(f'Execution time: {execution_time:.1f} s.')


#%% Load data
data = dict()
idx_observation = 0


for data_file in files_in_folder:
    
    with open(data_file, 'rb') as file:
        avro_reader = fastavro.reader(file)
        
        for idx_in_file, observation in enumerate(avro_reader):
            
            for attribute_observation in observation.keys():
                    
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

    
    
