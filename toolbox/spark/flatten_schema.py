from .flat_schema_mapping import flat_schema_mapping
from pyspark.sql.dataframe import DataFrame


def flatten_schema(df: DataFrame):
    """
    :param df: Spark dataframe
    :return: Spark dataframe with flat schema
    """
    # Define the mapping of the column names
    mapping = flat_schema_mapping(df)

    return df.select(*mapping)
