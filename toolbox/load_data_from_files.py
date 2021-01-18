import time

from pathlib import Path
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from typing import Union
from toolbox.spark import load_dataframe_from_parquet


def load_data_from_files(path: Union[str, Path],
                         spark: SparkSession,
                         method=None,
                         verbose=True) -> DataFrame:

    if verbose:
        print(f'Loading data from path: "{str(path)}".')

    # Get the start time for timing
    start_time = time.time()

    # Make sure the path is represented as a string
    path = str(path)

    # Get the extension
    extension = path.split('.')[-1]

    if method is None:
        method = extension

    # Load avro files.
    if method == 'avro':
        df = spark.read.format('avro').load(path)

    # Load parquet files.
    elif method == 'parquet':
        df = load_dataframe_from_parquet(path, spark, verbose=False)

    else:
        raise ValueError(f'Invalid method: "{method}".')

    if verbose:
        print(f'\tExecution time: {time.time() - start_time:.5f} s.')

    return df
