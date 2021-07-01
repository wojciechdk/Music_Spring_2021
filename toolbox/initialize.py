#%% Imports
from toolbox.config import Config
from toolbox.imports import *


#%% Options
# Display all outputs in cells.
InteractiveShell.ast_node_interactivity = 'all'

# In Pandas tables, display no more than 100 rows.
pd.set_option('display.max_rows', 100)


#%% Setup PySpark environment
avro_jar_path = '/data/work/shared/tools/spark-avro_2.12-3.0.0.jar'
spark_data_folder = '/data/work/shared/s001284/spark_tmp'
memory_driver = '20g'
memory_executor = '6g'
number_of_cores = 5

os.environ['PYSPARK_SUBMIT_ARGS'] = (
    f'--jars {avro_jar_path}'
    # f' --driver-cores {number_of_cores}'
    # f' --executor-cores {number_of_cores}'
    # f' --driver-memory {memory_driver}'
    # f' --executor-memory {memory_executor}'
    # f' --conf spark.local.dir=/{spark_data_folder}'
    f' pyspark-shell')