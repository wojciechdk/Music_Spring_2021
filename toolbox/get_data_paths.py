from toolbox.config import Config


def get_data_paths(verbose=True):

    if verbose:
        print(f'Getting the data paths.')

    data_root = Config.Path.data_root

    data_subfolders = [item
                       for item in data_root.iterdir()
                       if item.is_dir()]

    data_files = [item
                  for folder in data_subfolders
                  for item in folder.iterdir()
                  if item.is_file() and item.suffix == '.avro']

    if verbose:
        print(f'\tNumber of folders: {len(data_subfolders)}')
        print(f'\tNumber of files: {len(data_files)}')

    return data_root, data_subfolders, data_files
