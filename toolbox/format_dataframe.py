import time
import toolbox as t

from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as f


def format_dataframe(df: DataFrame,
                     verbose=True) -> DataFrame:
    if verbose:
        print(f'Formatting dataframe:')

    # Save start time for timing
    start_time = time.time()

    # Explode the columns: "devices" and "tracks"
    if verbose:
        print(f'\tExploding columns containing lists.')

    df = (
        df
        .withColumn('devices', f.explode_outer('devices'))
        .withColumn('tracks', f.explode_outer('tracks'))
    )

    # Flatten the schema.
    if verbose:
        print(f'\tFlattening the dataframe schema.')

    df = t.spark.flatten_schema(df)

    if verbose:
        print(f'\tExecution time: {time.time() - start_time:.5f}')

    return df
