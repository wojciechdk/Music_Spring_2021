from toolbox.initialize import *


#%% Initialize a Spark session.

# Save the start time for timing.
start_time = time.time()

#
spark = t.spark.create_session('Music_Activity')

# Set this for faster conversion from Spark to pandas.
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# Define the path to the data.
data_path = Config.Path.music_data_clean_root

# Load the data into a clean dataframe
df = t.load_data_from_files(data_path,
                            spark,
                            method='parquet')

#%%
df_pd = df.limit(5).toPandas()

#%%
d


#%%
# Save the dataframe with partitions defined by the first
# two letters of the user ID.
(df
 # .withColumn('user_id_prefix', f.col('user_id').substr(0, 2))
 .withColumnRenamed('track_id', 'spotify_uri')
 .withColumnRenamed('track_uri', 'sony_uri')
 .write.mode('overwrite')
 .parquet(str(Config.Path.music_data_clean_sample_1E5_root / 'df_clean_uri'))
)

# Print the execution time.
print(f'Execution time: {time.time() - start_time:.5f} s.')
