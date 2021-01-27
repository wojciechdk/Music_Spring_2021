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