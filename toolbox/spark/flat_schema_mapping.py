from .unfolded_columns import unfolded_columns
from pyspark.sql import functions as f
from pyspark.sql.dataframe import DataFrame


def flat_schema_mapping(df:  DataFrame):

    # Define the column names in the nested structure and their corresponding
    # values in a flat structure.
    column_names_nested_structure = unfolded_columns(df.schema)
    column_names_flat_structure = [name.replace('.', '_')
                                   for name
                                   in column_names_nested_structure]

    # Define the mapping of the column names
    mapping = [f.col(name_nested).alias(name_flat)
               for name_nested, name_flat
               in zip(column_names_nested_structure,
                      column_names_flat_structure)]

    return mapping
