from toolbox.initialize import *


#%% Initialize a Spark session.

# Save the start time for timing.
start_time = time.time()

#
spark = t.spark.create_session('Music_Activity')

# Set this for faster conversion from Spark to pandas.
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")


# Load the data into a clean dataframe
df = t.load_data_from_files(Config.Path.music_data_clean_root,
                            spark,
                            method='parquet')


# Save the dataframe with partitions defined by the first
# two letters of the user ID.
(df
 # .withColumn('user_id_prefix', f.col('user_id').substr(0, 2))
 .withColumnRenamed('track_duration', 'track_playback_duration')
 .withColumnRenamed('sony_uri', 'track_sony_uri')
 .withColumnRenamed('spotify_uri', 'track_spotify_uri')
 .write.mode('overwrite')
 .partitionBy('user_id_prefix')
 .parquet(str(Config.Path.project_data_root / 'df_clean_newest'))
)

# Print the execution time.
print(f'Execution time: {time.time() - start_time:.5f} s.')
