import time

from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame


def count_rows(df: DataFrame, verbose=True):
    """
    Counts the rows of a Spark Dataframe and prints out the result and
    execution time.

    :param df:
    :param verbose:
    :return:
    """

    if verbose:
        print(f'Counting the rows.')

    # Save start time for timing.
    start_time = time.time()

    number_of_records = df.count()
    print(f'\tNumber of records: {number_of_records}.')

    if verbose:
        print(f'\tExecution time: {time.time() - start_time:.5f} s.')

    return number_of_records
