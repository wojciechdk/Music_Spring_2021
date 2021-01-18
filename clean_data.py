#%% Imports
from toolbox.config import Config
from toolbox.imports import *


#%% Initialize a Spark session
spark = t.spark.create_session('Music_Activity')


#%% Load sample dataframe
path_df_1E5 = Config.Path.project_data_root / 'df_sample_1E5.parquet'
path_df_1E6 = Config.Path.project_data_root / 'df_sample_1E6.parquet'

df_1E5 = t.spark.load_dataframe_from_parquet(path_df_1E5, spark)
df_1E6 = t.spark.load_dataframe_from_parquet(path_df_1E6, spark)


#%% Cache
df_1E5.cache()
df_1E6.cache()


#%% Count the rows
number_of_rows_df_1E5 = t.spark.count_rows(df_1E5)
number_of_rows_df_1E6 = t.spark.count_rows(df_1E6)


#%% Convert to pandas
df_sample_pd = df_1E5.limit(100).toPandas()





#%%
# Compute dataframe with number of listings and neighbourhoods per city.
# Order by number of listings (bigger first).
df_listings_neighborhoods_per_city = (
    df_1E5
    .where(f.col("neighbourhood").isNotNull()) # Disregard rows where neighbourhood is not specified.
    .groupBy('city')
    .agg(
        f.countDistinct('id').alias('number_of_listings'),  # Count the number of listings (IDs)
        f.countDistinct('neighbourhood').alias('number_of_neighbourhoods')  # Count the number of neighborhoods
        )
    .orderBy(f.desc('number_of_listings'))
    )


#%%
data_root, data_subfolders, data_files = t.get_data_paths()

with open(data_files[0], 'rb') as file:
    avro_reader = fastavro.reader(file)

    column_docs = t.get_avro_docs(avro_reader.writer_schema)

print(column_docs)

