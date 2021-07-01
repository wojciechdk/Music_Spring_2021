import time

from pyspark.sql.session import SparkSession


def create_session(app_name='',
                   verbose=True,
                   memory_fraction=0.6,
                   driver_memory='20g',
                   executor_memory='6g',
                   local_dir='/data/work/shared/s001284/spark_tmp',
                   number_of_cores=5):
    '''
    The function creates a PySpark session with default parameters geared
    towards the use at DTU

    :param app_name:
    :param verbose:
    :param memory_fraction:
    :param driver_memory:
    :param executor_memory:
    :param local_dir:
    :param number_of_cores:
    :return:
    '''

    if verbose:
        print(f'Creating a Spark session.')

    # Get the start time for timing
    start_time = time.time()

    spark = (
        SparkSession.builder
            .appName(app_name)
            .config('spark.memory.fraction', str(memory_fraction))
            .config('spark.driver.memory', driver_memory)
            .config('spark.executor.memory', executor_memory)
            .config('spark.local.dir', local_dir)
            .config('spark.sql.files.ignoreCorruptFiles', 'true')
            .config('spark.sql.session.timeZone', 'UTC')
            .master(f'local[{number_of_cores}]')
            .getOrCreate()
    )

    if verbose:
        print(f'\tExecution time: {time.time() - start_time:.5f} s.')

    return spark
