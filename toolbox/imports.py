import datetime
import fastavro
import importlib
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pandavro
import pyspark
import os
import seaborn as sns
import toolbox as t
import time
import wojciech as w

from IPython.core.interactiveshell import InteractiveShell
from pathlib import Path
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as f
from pyspark.sql.avro.functions import from_avro, to_avro
from pyspark.sql.types import ArrayType, BooleanType, FloatType, IntegerType, \
    LongType, NullType, StringType, StructType, StructField, TimestampType
from tqdm import tqdm


# Setup PySpark environment
avro_jar_path = '/data/work/shared/tools/spark-avro_2.12-3.0.0.jar'
spark_data_folder = '/data/work/shared/s001284/spark_tmp'
memory_driver = '20g'
memory_executor = '6g'
number_of_cores = 5

os.environ['PYSPARK_SUBMIT_ARGS'] = (
    f'--jars {avro_jar_path}'
    # f' --driver-cores {number_of_cores}'
    # f' --executor-cores {number_of_cores}'
    # f' --driver-memory {memory_driver}'
    # f' --executor-memory {memory_executor}'
    # f' --conf spark.local.dir=/{spark_data_folder}'
    f' pyspark-shell')
