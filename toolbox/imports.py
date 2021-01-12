#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan  8 20:13:40 2021

@author: s001284
"""

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