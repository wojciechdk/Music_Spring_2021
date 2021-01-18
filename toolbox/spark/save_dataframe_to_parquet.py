import time

from pathlib import Path
from pyspark.sql.dataframe import DataFrame
from typing import Union


def save_dataframe_to_parquet(df: DataFrame,
                              path_save: Union[Path, str],
                              verbose=True):

    if verbose:
        print(f'Saving to "{str(path_save)}".')

    # Note the start time for timing of the operation.
    start_time = time.time()

    # Save RDD as pickle
    df.write.parquet(str(path_save))

    if verbose:
        print(f'\tExecution time: {time.time() - start_time:.5f} s.')
