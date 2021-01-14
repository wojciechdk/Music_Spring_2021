from pathlib import Path


class Config:

    class Path:
        # Project root
        project_root = Path(__file__).parent.parent

        # Data paths
        data_root = Path('/data/work/src/musicactivity/')
