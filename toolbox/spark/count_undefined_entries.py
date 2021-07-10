from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from wojciech.printmd import printmd


def count_undefined_entries(df: DataFrame,
                            column_name: str,
                            verbose=False):
    '''
    Counts the number of undefined (null) entries in a Spark dataframe and
    prints the result if desired.

    :param df:
    :param column_name:
    :param verbose:
    :return:
    '''

    # Count the undefined (null entries)
    n_undefined = (
        df
        .where(f.col(column_name).isNull())
        .count()
    )

    # Display the result.
    if verbose == 'markdown':
        printmd(f'Number of undefined entries in the column: '
                f'`{column_name}`: **{n_undefined:,.0f}**')

    elif verbose:
        printmd(f'Number of undefined entries in the column: '
                f'"{column_name}": {n_undefined:,.0f}')

    return n_undefined
