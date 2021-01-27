import matplotlib.pyplot as plt

from pathlib import Path
from toolbox.config import Config
from typing import Union


def save_plot(file_name: Union[Path, str],
              instance: plt.Figure = None,
              verbose=True):
    """
    Saves the plot to the report folder

    :param path: a pathlib path or a string representing the name of the saved
                 file
    :param instance: matplotlib Figure to save. If undefined, the current plot
                     is saved.
    :param verbose: Print messages to user (True / False). Default True
    :return: None
    """

    if instance is not None:
        instance.savefig(Config.Path.report_root / file_name)
    else:
        plt.savefig(Config.Path.report_root / file_name)
