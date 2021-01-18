#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan  8 20:13:40 2021

@author: s001284
"""

import fastavro
import numpy as np
import pandas as pd
import pandavro
import pyspark
import os
import toolbox as t
import time
import wojciech as w

from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.avro.functions import from_avro, to_avro
from pyspark.sql.types import ArrayType, BooleanType, FloatType, IntegerType, \
    NullType, StringType, StructType, StructField

# Setup PySpark avro
import os
jar_path = '/data/work/shared/tools/spark-avro_2.12-3.0.0.jar'
os.environ['PYSPARK_SUBMIT_ARGS'] = f'--jars {jar_path} pyspark-shell'
