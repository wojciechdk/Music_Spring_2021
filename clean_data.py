#%% Imports
from toolbox.config import Config
from toolbox.imports import *


#%% Initialize a Spark session.
spark = t.spark.create_session('Music_Activity')


#%% Get the paths of data files and folders.
data_root, data_subfolders, data_files = t.get_paths_raw_data()


#%% Load the raw data.
df_raw = t.load_data_from_files(data_root, spark, method='avro')
df_full = t.format_dataframe(df_raw)

number_of_records = 3571278161



#%% Load sample dataframe.
path_df_1E5 = Config.Path.project_data_root / 'df_sample_1E5.parquet'
path_df_1E6 = Config.Path.project_data_root / 'df_sample_1E6.parquet'

df_1E5 = t.load_data_from_files(path_df_1E5, spark)
df_1E6 = t.load_data_from_files(path_df_1E6, spark)


#%% Cache
df_1E5.cache()
df_1E6.cache()

number_of_rows_df_1E5 = t.spark.count_rows(df_1E5)
number_of_rows_df_1E6 = t.spark.count_rows(df_1E6)


#%% Convert to pandas
df_sample_pd = df_1E5.limit(100).toPandas()


#%%
df_n_activities = (
    df_1E5
    .groupBy('id')
    .agg(f.countDistinct('id').alias('number_of_activities'))
    .orderBy(f.desc('number_of_activities'))
)

df_n_activities.show()


#%%
df_n_device_types = (
    df_1E6
    .groupBy(f.col('devices_type'))
    .count()
    .orderBy(f.desc('count'))
)

df_n_device_types.show()




#%% Device types in the full dataframe
start_time = time.time()

(df_full
 .groupBy(f.col('devices_type'))
 .count()
 .orderBy(f.desc('count'))
 .show()
)

print(f'Execution time: {time.time() - start_time:.5f} s.')