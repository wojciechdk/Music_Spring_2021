#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan  8 20:46:23 2021

@author: s001284
"""
#%% Imports
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import ArrayType, BooleanType, FloatType, IntegerType, \
    StringType, StructType, StructField


#%% Define the function
def test_dataframe(spark: SparkSession):
    # Create the column layout.
    schema = StructType([
        StructField('name', StructType([
            StructField('first', StringType(), nullable=True),
            StructField('middle', StringType(), nullable=True),
            StructField('last', StringType(), nullable=True),
            ])),
        StructField('age', IntegerType(), nullable=True),
        StructField('gender', StringType(), nullable=True),
        ])
    
    # Define data
    data = [
        (('Wojciech', 'Jaroslaw', 'Mazurkiewicz'), 38, 'M'),
        (('Martina', 'Bo', 'Rubino'), 33, 'F'),
        (('Mia', '', 'Rubino'), 2, 'F'),
        (('Leo', '', 'Rubino'), 1, 'M')   
    ]

    # Create the dataframe
    df = spark.createDataFrame(data=data, schema=schema)

    return df
