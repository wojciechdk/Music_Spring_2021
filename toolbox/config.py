from pathlib import Path


class Config:

    class Path:
        # Project root
        project_root = Path(__file__).parent.parent

        # Resources
        resources_root = project_root / 'resources'
        project_data_root = resources_root / 'data'
        project_stats_root = resources_root / 'stats'

        # Documentation
        documentation_root = project_root / 'documentation'

        # Path to the folder containing the raw music activity data
        music_data_raw_root = Path('/data/work/src/musicactivity/')
        music_data_raw_sample_1E5_root = \
            project_data_root / 'df_raw_sample_1E5.parquet'
        music_data_raw_sample_1E6_root = \
            project_data_root / 'df_raw_sample_1E6.parquet'

        # Path to the folder containing the clean music activity data
        music_data_clean_root = project_data_root / 'df_clean'
        music_data_clean_sample_1E5_root = \
            project_data_root / 'df_clean_sample_1E5.parquet'
        music_data_clean_sample_1E6_root = \
            project_data_root / 'df_clean_sample_1E6.parquet'

        # Initial stats
        initial_stats_df_n_undefined = project_stats_root / 'df_n_undefined.pkl'
        initial_stats_df_n_distinct = project_stats_root / 'df_n_distinct.pkl'
        initial_stats_df_combined_stats = \
            project_stats_root / 'df_combined_stats.pkl'

        initial_stats_playbacks_per_activity_counts = \
            project_stats_root / 'playbacks_per_activity_counts.npy'

        # Report
        report_root = documentation_root / 'report'
        report_resources_root = report_root / 'src' / 'resources'
        report_images_root = report_resources_root / 'images'
        report_tables_root = report_resources_root / 'tables'

