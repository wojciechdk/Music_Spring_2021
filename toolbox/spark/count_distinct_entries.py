from pyspark.sql import DataFrame
from wojciech.printmd import printmd


def count_distinct_entries(df: DataFrame,
                           column_name: str,
                           verbose=False):
    '''
    Counts the number of distinct entries in a Spark dataframe and
    prints the result if desired.

    :param df:
    :param column_name:
    :param verbose:
    :return:
    '''

    # Count the distinct entries
    n_distinct = (
        df
        .select(column_name)
        .distinct()
        .count()
    )

    # Display the result.
    if verbose == 'markdown':
        printmd(
            f'Number of distinct entries in the column: '
            + f'`{column_name}`: **{n_distinct:,.0f}**'
        )

    elif verbose:
        printmd(
            f'Number of undefined entries in the column: '
            + f'"{column_name}": {n_distinct:,.0f}'
        )

    return n_distinct
