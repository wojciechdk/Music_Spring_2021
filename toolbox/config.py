from pathlib import Path


class Config:

    class Path:
        # Project root
        project_root = Path(__file__).parent.parent

        # Path to the folder containing the music activity data
        music_data_root = Path('/data/work/src/musicactivity/')

        # Resources folder
        resources_root = project_root / resources

        # Project data folder
        project_data_root = resources_root / data
