
from pyspark.sql.types import StructType

#%%


def get_schema_doc(writer_schema: dict, prefix=''):
    """
    Get the doc for each column in an avro writer schema.

    :param writer_schema:
    :param prefix:
    :return: list of tuples: (column_name, column_doc)
    """

    column_docs = list()

    for field in writer_schema['fields']:

        # Get the column name and corresponding doc for the column at hand.
        column_name = prefix + field['name']

        if 'doc' in field.keys():
            column_doc = field['doc']
        else:
            column_doc = ''

        column_docs.append((column_name, column_doc))

        # Get the column name and corresponding doc for the subcolumns.
        if 'items' in field['type'].keys():
            column_docs += get_schema_doc(field['type']['items'],
                                          prefix=column_name + '_')

    return column_docs
