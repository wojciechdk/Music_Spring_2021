import matplotlib.pyplot as plt
import pandas as pd

from pathlib import Path
from toolbox.config import Config
from typing import Union


def save_table(df: pd.DataFrame,
               table_name: str,
               verbose=True):
    """
    Saves the pandas dataframe in LaTeX format to the report tables folder

    :param table_name: a string representing the name of the saved table
    :param df: pandas dataframe to save.
    :param verbose: Print messages to user (True / False). Default True
    :return: None
    """

    path = Config.Path.report_tables_root / (table_name + '.tex')

    if verbose:
        print(f'Saving table to: "{str(path)}".')

    latex_table = df.to_latex()

    with open(path, 'w') as file:
        file.write(latex_table)

    if verbose:                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 
        print(f'\tDone.')
