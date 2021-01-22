#%% Imports
from toolbox.config import Config
from toolbox.imports import *


#%% Options
# Display all outputs in cells.
InteractiveShell.ast_node_interactivity = 'all'

# In Pandas tables, display no more than 100 rows.
pd.set_option('display.max_rows', 100)


#%% Initialize Spark Session

spark = t.spark.create_session('Music_Activity_Jupyter')


#%% Get the dataframes containing data in the raw format

# Get the full dataset
data_root_raw, data_subfolders_raw, data_files_raw = t.get_paths_raw_data()
df_raw = t.load_data_from_files(data_root_raw, spark, method='avro')

# Get the sample data
path_df_sample_raw_1E6 = \
    Config.Path.project_data_root / 'df_sample_raw_1E6.parquet'

df_sample_raw_1E6 = t.load_data_from_files(path_df_sample_raw_1E6, spark)