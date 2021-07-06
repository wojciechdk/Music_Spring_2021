from toolbox.initialize import *


#%% Initialize a Spark session.

# Save the start time for timing.
start_time = time.time()

#
spark = t.spark.create_session('Music_Activity')

# Set this for faster conversion from Spark to pandas.
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

#%%
# Load the data into a clean dataframe
df = t.load_data_from_files(Config.Path.music_data_clean_root,
                            spark,
                            method='parquet')

#%%
# n_rows = df.count()
n_rows = 2530475843
n_rows_sample = 1e6

# Save the dataframe with partitions defined by the first
# two letters of the user ID.
(df
 .sample(fraction=n_rows_sample / n_rows)
 .write.mode('overwrite')
 .parquet(str(Config.Path.music_data_clean_sample_1E6_root))
)

# Print the execution time.
print(f'Execution time: {time.time() - start_time:.5f} s.')
