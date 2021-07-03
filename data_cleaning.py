#%% Initialization

# In[1]:
from toolbox.initialize import *


#%% Create a Spark Session

# Initialize a Spark session.
spark = t.spark.create_session('Music_Activity')

# Set this for faster conversion from Spark to pandas.
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")


#%% Create a Spark SessionLoad data

# Get the full dataset
data_root_raw, data_subfolders_raw, data_files_raw = t.get_paths_raw_data()
df_raw = t.load_data_from_files(data_root_raw, spark, method='avro')

# Get a database containing 1e6 randomly chosen samples.
df_sample_raw_1E6 = t.load_data_from_files(
    Config.Path.music_data_raw_sample_1E6_root, spark)

# In[2]:

# Choose whether to show the results of the steps leading to the final result.
display_middle_results = False


# ### Removing rows where activity has been deleted

# In[3]:

# Save the start time for timing.
start_time = time.time()

# Keep only the rows where "deleted_time" is not defined.
df_clean = (
    df_raw
    .where(f.col('deleted_time').isNull()
           | (f.col('deleted_time') == ''))
)

# Show the top rows of the resulting dataframe.
if display_middle_results:
    display(df_clean.limit(100).toPandas().head())

# Show the execution time.
print(f'Execution time: {time.time() - start_time:.5f} s.')


# ### Dropping duplicates of activity id (keeping the most recent)

# In[4]:


# Save the start time for timing
start_time = time.time()

# Define the time format used for the time stamp - strings in the
# raw database.
time_format = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"

# Define a partitioning by activity id.
window_id = Window.partitionBy('id')

# Drop rows containing the duplicates of the activity ID.
# Keep the activity ID with the latest timestamp-
df_clean = (
    df_clean
    
    # Convert the start time to timestamp.
    .withColumn('start_time',
                f.to_timestamp('start_time', time_format)) 
    
    # Save the latest start time for each activity ID in a separate column.
    .withColumn('latest_start_time', f.max('start_time').over(window_id))
    
    # Keep only rows with the latest start time for the given activity ID.
    .where(f.col('start_time') == f.col('latest_start_time'))
        
    # Keep only the first occurrence of each activity ID.
    .dropDuplicates(['id'])
    
    # Delete the column containing the latest start time.
    .drop('latest_start_time')
)

# Show the top rows of the resulting dataframe.
if display_middle_results:
    df_clean.limit(100).toPandas().head()
    
# Print the execution time.
print(f'Execution time: {time.time() - start_time:.5f} s.')


# ### Exploding the columns `devices` and `tracks` and flattening the schema

# In[5]:


# Explore the column devices and tracks
# and flatten the schema.
df_clean = t.format_dataframe(df_clean)

# Show the top rows of the resulting dataframe.
if display_middle_results:
    display(df_clean.limit(100).toPandas().head())


# ### Replacing start and end time with start time and duration

# In[6]:


# Save the start time for timing
start_time = time.time()

# Define the time format used for the time stamp - strings in the
# raw database.
time_format = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"

df_clean = (
    df_clean
#     .withColumn('start_time',
#                 f.to_timestamp('start_time', time_format))
    .withColumn('end_time',
                f.to_timestamp('end_time', time_format))
    .withColumn('activity_duration',
                f.col('end_time').cast(LongType()) 
                - f.col('start_time').cast(LongType()))   
    .withColumn('tracks_start_time',
                f.to_timestamp('tracks_start_time', time_format))
    .withColumn('tracks_end_time',
                f.to_timestamp('tracks_end_time', time_format))
    .withColumn('track_duration',
                f.col('tracks_end_time').cast(LongType()) 
                - f.col('tracks_start_time').cast(LongType()))
    .drop('end_time')
    .drop('tracks_end_time')
)

# Show the top rows of the resulting dataframe.
if display_middle_results:
    display(df_clean.limit(100).toPandas().head())
    
# Print the execution time.
print(f'Execution time: {time.time() - start_time:.5f} s.')


# ### Merging Track ID and Track URI

# I can't decide whether it's a good idea or not. `tracks_id` seems to be a path to a file in the Sony system, which I can't see give any useful information, maybe aside of comparing the track properties of multiple rows with the same track id to look for inconsitencies. I will be dropping the merge for now.

# In[7]:


# df_clean = (
#     df_clean
#     .withColumn('track_id',
#                 f.concat(f.col('tracks_id'), 
#                          f.col('tracks_uri')))
#     .drop('tracks_id')
#     .drop('tracks_uri')
# )

# df_clean.limit(3).toPandas().head()


# ### Drop redundant columns

# Let's remove the columns:
# 
# * `deleted_time`
# * `devices_type`
# * `yearmonth`

# In[8]:

# Drop the redundant columns.
df_clean = (
    df_clean
    .drop('deleted_time')
    .drop('devices_type')
    .drop('yearmonth')
)

# Show the top rows of the resulting dataframe.
if display_middle_results:
    df_clean.limit(100).toPandas().head()


# ### Rename and organize columns

# In[9]:


# Save the start time for timing.
start_time = time.time()

df_clean = (
    df_clean
    
    # Give columns meaningful names.
    .withColumnRenamed('id', 'activity_id')
    .withColumnRenamed('useruuid', 'user_id')
    .withColumnRenamed('start_time', 'activity_start_time')
    .withColumnRenamed('devices_name', 'device_name')
    .withColumnRenamed('devices_id', 'device_id')
    .withColumnRenamed('tracks_start_time', 'track_start_time')
    .withColumnRenamed('tracks_artist', 'track_artist')
    .withColumnRenamed('tracks_album', 'track_album')
    .withColumnRenamed('tracks_title', 'track_title')
    .withColumnRenamed('tracks_player', 'track_player')
    .withColumnRenamed('tracks_id', 'track_id')
    .withColumnRenamed('tracks_uri', 'track_uri')
    
    # Place the columns belonging to the same group next to each other.
    .select('user_id',
            'activity_id', 'activity_start_time', 'activity_duration',
            'device_id', 'device_name',
            'track_artist', 'track_title', 'track_album',
            'track_player', 'track_start_time', 'track_duration',
            'track_id', 'track_uri')
    
    # Sort the data.
    .orderBy(f.asc('user_id'),
             f.asc('activity_start_time'))
)

# Show the top rows of the resulting dataframe.
# df_clean.limit(100).toPandas().head()
    
# Print the execution time.
print(f'Execution time: {time.time() - start_time:.5f} s.')


# <hr style="border:2px solid black"></hr>
# 
# # Save the dataframe

# In[10]:


# Save the start time for timing.
start_time = time.time()

# Save the dataframe with partitions defined by the first
# two letters of the user ID.
(df_clean
 .withColumn('user_id_prefix', f.col('user_id').substr(0, 2))
 .write.mode('overwrite')
 .partitionBy('user_id_prefix')
 .parquet(str(Config.Path.project_data_root / 'df_clean'))
)

# Print the execution time.
print(f'Execution time: {time.time() - start_time:.5f} s.')
