import pandas
import fastavro


def avro_to_pandas(filepath, encoding='rb'):
    # Open file stream
    with open(filepath, encoding) as file:
        # Configure Avro reader
        reader = fastavro.reader(file)

        # Load records in memory
        records = [record for record in reader]

        # Populate pandas.DataFrame with records
        df = pandas.DataFrame.from_records(records)

        # Return created DataFrame
        return df
