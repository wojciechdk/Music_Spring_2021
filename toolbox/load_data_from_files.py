import time

from pathlib import Path
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from typing import Union


def load_data_from_files(path: Union[str, Path],
                         spark: SparkSession,
                         verbose=True) -> DataFrame:

    if verbose:
        print(f'Loading data from path: "{str(path)}".')

    # Get the start time for timing
    start_time = time.time()

    # Load the data into a Spark dataframe
    df = spark.read.format('avro').load(str(path))

    if verbose:
        print(f'\tExecution time: {time.time() - start_time:.5f}')

    return df
