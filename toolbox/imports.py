import fastavro
import numpy as np
import pandas as pd
import pandavro
import pyspark
import os
import toolbox as t
import time
import wojciech as w

from IPython.core.interactiveshell import InteractiveShell
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.avro.functions import from_avro, to_avro
from pyspark.sql.types import ArrayType, BooleanType, FloatType, IntegerType, \
    LongType, NullType, StringType, StructType, StructField, TimestampType

# Setup PySpark en
avro_jar_path = '/data/work/shared/tools/spark-avro_2.12-3.0.0.jar'
memory = '10g'
os.environ['PYSPARK_SUBMIT_ARGS'] = (
    f'--jars {avro_jar_path}'
    f' --driver-memory {memory}'
    f' --executor-memory {memory}'
    f' pyspark-shell')