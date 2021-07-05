# %%
from toolbox.initialize import *

import pyspark.sql.functions as f

# Initialize a Spark session.
spark = t.spark.create_session('Test_WR')

# Set this for faster conversion from Spark to pandas.
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# %%
# Create the column layout.
schema = StructType([
    StructField('name', StructType([
        StructField('first', ArrayType(StringType()), nullable=True),
        StructField('middle', ArrayType(StringType()), nullable=True),
        StructField('last', ArrayType(StringType()), nullable=True),
    ])),
    StructField('age', IntegerType(), nullable=True),
    StructField('gender', StringType(), nullable=True),
])

# Define data
data = [
    ((['Wojciech'], ['Jaroslaw'], ['Mazurkiewicz']), 38, 'M'),
    ((['Martina'], ['Bo'], ['Rubino']), 33, 'F'),
    ((['Mia'], [''], ['Rubino']), 2, 'F'),
    ((['Leo'], [''], ['Rubino']), 1, 'M')]

# Create the dataframe
df = spark.createDataFrame(data=data, schema=schema)

df.show()
df.printSchema()
#
df = (
    df.withColumn('name', f.explode_outer('name'))
)
df.show()


# display(df.toPandas())
