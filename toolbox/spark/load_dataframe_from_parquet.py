import time

from pathlib import Path
from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame
from typing import Union


def load_dataframe_from_parquet(path_load: Union[Path, str],
                                spark: SparkSession,
                                verbose=True) -> DataFrame:

    if verbose:
        print(f'Loading dataframe from "{path_load}".')

    # Represent the path as a string:
    path_load = str(path_load)

    # Note the start time for timing of the operation.
    start_time = time.time()

    # Load dataframe
    df_from_parquet = spark.read.parquet(path_load)

    if verbose:
        print(f'\tExecution time: {time.time() - start_time:.5f} s.')

    return df_from_parquet
