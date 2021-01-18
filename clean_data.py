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



#%% Count the rows
number_of_rows_df_1E5 = t.spark.count_rows(df_1E5)
number_of_rows_df_1E6 = t.spark.count_rows(df_1E6)

#%%
