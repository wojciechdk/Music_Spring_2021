import time

from pyspark.sql.session import SparkSession


def create_session(app_name='', verbose=True):
    if verbose:
        print(f'Creating a Spark session.')

    # Get the start time for timing
    start_time = time.time()

    spark = (
        SparkSession.builder
            .appName(app_name)
            .config('spark.memory.fraction', '0.6')
            .config('spark.driver.memory', '20g')
            .config('spark.executor.memory', '6g')
            .config('spark.local.dir', '/data/work/shared/s001284/spark_tmp')
            .config('spark.sql.files.ignoreCorruptFiles', 'true')
            .config('spark.sql.session.timeZone', 'UTC')
            .master('local[5]')
            .getOrCreate()
    )

    if verbose:
        print(f'\tExecution time: {time.time() - start_time:.5f} s.')

    return spark
