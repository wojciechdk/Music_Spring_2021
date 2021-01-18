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


from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import ArrayType, BooleanType, FloatType, IntegerType, \
    NullType, StringType, StructType, StructField 


# Initialize a spark session
spark = (SparkSession.builder
         .appName('Wojciech_test')
         .getOrCreate())


#%% Create a test dataframe

# Create a test dataframe
df = spark.createDataFrame(*t.test_dataframe())

df.printSchema()
df.show()


#%% Spark NullType
# Create the colum layout.
schema = StructType([
    StructField('A', StringType(), nullable=True),
    StructField('B', StringType(), nullable=True),
    ])

# Define data
data = [
    'lala_1', None,
    'lala_2', None
]

# Create a test dataframe
df = spark.createDataFrame(data, schema)
df.printSchema()
df.show()
