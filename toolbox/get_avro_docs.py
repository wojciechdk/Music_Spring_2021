import fastavro
import toolbox as t

from pathlib import Path
from typing import Union


def get_avro_docs(path_avro_file: Union[str, Path]):
    """
    Returns the docs of the writer schema of an avro file.

    :param path_avro_file: Path to the avro file
    :return: List of tuples: (column_name, column_doc)
    """

    with open(str(path_avro_file), 'rb') as file:
        avro_reader = fastavro.reader(file)

        return t.get_schema_docs(avro_reader.writer_schema)