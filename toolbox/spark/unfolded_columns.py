from pyspark.sql.types import StructType


def unfolded_columns(df_schema: StructType, prefix=''):
    """
    Returns all the columns (including the nested columns) of a given schema.

    :param df_schema: Schema of a Spark dataframe.
    :param prefix: Should be left unspecified. For use in recursion.
    :return: A list with all the column names
    """

    column_names = list()

    for column in df_schema.fields:
        if isinstance(column.dataType, StructType):
            new_prefix = prefix + column.name + '.'

            column_names += [prefix + column_name
                             for column_name
                             in unfolded_columns(column.dataType,
                                                 prefix=new_prefix)]

        else:
            column_names.append(prefix + column.name)

    return column_names
