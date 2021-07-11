from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from wojciech.printmd import printmd


def count_undefined_entries(df: DataFrame,
                            column_name: str,
                            verbose=False,
                            n_rows=None):
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

    # Initialize the string representing the number of distinct values as a
    # percentage of total values.
    str_n_undefined_pct = ''

    if n_rows is not None:
        n_undefined_pct = n_undefined / n_rows * 100

        if verbose == 'markdown':
            str_n_undefined_pct = f' **({n_undefined_pct:.2f}%)**'

        elif verbose:
            str_n_undefined_pct = f' ({n_undefined_pct:.2f}%)'

    # Display the result.
    if verbose == 'markdown':
        printmd(
            f'Number of undefined entries in the column: '
            + f'`{column_name}`: **{n_undefined:,.0f}**'
            + str_n_undefined_pct

        )

    elif verbose:
        printmd(
            f'Number of undefined entries in the column: '
            + f'"{column_name}": {n_undefined:,.0f}'
            + str_n_undefined_pct
        )

    if n_rows is None:
        return n_undefined
    else:
        return n_undefined, n_undefined_pct

