from pyspark.sql import DataFrame


def get_column(df: DataFrame, column_name: str):
    return df.select(column_name).rdd.flatMap(lambda x: x).collect()
