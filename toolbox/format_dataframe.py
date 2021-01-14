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
    df = (
        df
        .withColumn('devices', f.explode_outer('devices'))
        .withColumn('tracks', f.explode_outer('tracks'))
    )

    # Flatten the schema.
    df = t.spark.flatten_schema(df)

    if verbose:
        print(f'Execution time: {time.time() - start_time:.5f}')

    return df
