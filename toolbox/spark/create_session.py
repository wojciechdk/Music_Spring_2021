import time

from pyspark.sql.session import SparkSession


def create_session(app_name='', verbose=True):

    if verbose:
        print(f'Creating a Spark session.')

    # Get the start time for timing
    start_time = time.time()

    spark = (SparkSession.builder
             .appName(app_name)
             .getOrCreate())

    if verbose:
        print(f'\tExecution time: {time.time() - start_time:.5f} s.')

    return spark
