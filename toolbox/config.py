from pathlib import Path


class Config:

    class Path:
        # Project root
        project_root = Path(__file__).parent.parent

        # Path to the folder containing the music activity data
        music_data_root = Path('/data/work/src/musicactivity/')

        # Resources
        resources_root = project_root / 'resources'
        project_data_root = resources_root / 'data'

        # Documentation
        documentation_root = project_root / 'documentation'

        # Report
        report_root = documentation_root / 'report'
        report_resources_root = report_root / 'resources'
        report_images_root = report_resources_root / 'images'
        report_tables_root = report_resources_root / 'tables'

