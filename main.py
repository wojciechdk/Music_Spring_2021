#%% Imports
from toolbox.config import Config
from toolbox.imports import *


#%% Initialize a Spark session
spark = t.spark.create_session('Music_Activity')


#%% Get the paths of data files and folders
data_root, data_subfolders, data_files = t.get_data_paths()


#%% Load data from files
df_raw = t.load_data_from_files(data_root, spark, method='avro')


#%% Format the dataframe and its data
df = t.format_dataframe(df_raw)

number_of_records = 3571278161


#%% Create a sample dataframe

start_time = time.time()

n_samples = 1e6

df_sample = df.sample(fraction=n_samples / number_of_records,
                      withReplacement=False)

print(f'Execution time: {time.time() - start_time:.5f} s.')


#%% Save dataframe
t.spark.save_dataframe_to_parquet(
    df_sample, Config.Path.project_data_root / 'df_sample_1E6_2.parquet')


#%% Load dataframe


#%%




#%% Count the number of records
start_time = time.time()

number_of_records = df.count()
print(f'Number of records: {number_of_records}.')

print(f'Execution time: {time.time() - start_time:.5f}')




#%% Get the open
with open(data_files[0], 'rb') as file:
    avro_reader = fastavro.reader(file)

    for idx_in_file, observation in enumerate(avro_reader):
        print(observation)

        if idx_in_file >= 5:
            break


#%%
start_time = time.time()

### CODE HERE

print(f'Execution time: {time.time() - start_time:.5f}')
