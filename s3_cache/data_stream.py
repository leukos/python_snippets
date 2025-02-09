import pandas as pd
import os

from datetime import datetime
from s3_cache.s3_cache import S3FileSystem


class CSVDataStream:
    def __init__(self, stream_name, base_folder, prefix="", suffix=""):
        self.stream_name = stream_name
        self.base_folder = base_folder
        self.prefix = prefix
        self.suffix = suffix
        
    def get_data_for_date(self, date):
        """Retrieve data for a single date as DataFrame"""
        file_path = self._build_file_path(date)
        if os.path.exists(file_path):
            return pd.read_csv(file_path)
        return pd.DataFrame()

    def get_data_for_range(self, start_date, end_date):
        """Retrieve combined data for a date range"""
        dates = pd.date_range(start=start_date, end=end_date)
        return self._load_multiple_files(dates)

    def get_data_for_dates(self, dates):
        """Retrieve combined data for specific dates"""
        return self._load_multiple_files(dates)

    def _build_file_path(self, date):
        """Construct file path from components"""
        date_str = pd.to_datetime(date).strftime('%Y-%m-%d')
        filename = f"{self.prefix}{date_str}{self.suffix}.csv"
        return os.path.join(self.base_folder, filename)

    def _load_multiple_files(self, dates):
        """Load and combine multiple CSV files"""
        dfs = []
        for date in dates:
            file_path = self._build_file_path(date)
            if os.path.exists(file_path):
                dfs.append(pd.read_csv(file_path))
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    


class CachedCSVDataStream:
    def __init__(self, stream_name, base_folder, prefix="", suffix="", cache_dir="s3_cache"):
        """
        Initialize cached CSV data stream handler
        
        :param stream_name: Arbitrary identifier for the data stream
        :param base_folder: S3 path in format 'bucket-name/path/prefix'
        :param prefix: Filename prefix before date
        :param suffix: Filename suffix after date
        :param cache_dir: Local directory for cached files
        """
        self.stream_name = stream_name
        self.prefix = prefix
        self.suffix = suffix
        
        # Parse bucket and path prefix from base_folder
        if '/' in base_folder:
            self.bucket, self.path_prefix = base_folder.split('/', 1)
            if not self.path_prefix.endswith('/'):
                self.path_prefix += '/'
        else:
            self.bucket = base_folder
            self.path_prefix = ''
            
        # Initialize cache
        self.cache = S3FileCache(
            bucket_name=self.bucket,
            cache_dir=cache_dir
        )

    def get_data_for_date(self, date):
        """Retrieve data for a single date as DataFrame"""
        try:
            s3_key = self._build_s3_key(date)
            local_path = self.cache.get_file(s3_key)
            return pd.read_csv(local_path)
        except FileNotFoundError:
            return pd.DataFrame()

    def get_data_for_range(self, start_date, end_date):
        """Retrieve combined data for a date range"""
        dates = pd.date_range(start=start_date, end=end_date)
        return self._load_multiple_files(dates)

    def get_data_for_dates(self, dates):
        """Retrieve combined data for specific dates"""
        return self._load_multiple_files(dates)

    def _build_s3_key(self, date):
        """Construct S3 key from components"""
        date_str = pd.to_datetime(date).strftime('%Y-%m-%d')
        filename = f"{self.prefix}{date_str}{self.suffix}.csv"
        return f"{self.path_prefix}{filename}"

    def _load_multiple_files(self, dates):
        """Load and combine multiple CSV files"""
        dfs = []
        for date in dates:
            try:
                s3_key = self._build_s3_key(date)
                local_path = self.cache.get_file(s3_key)
                dfs.append(pd.read_csv(local_path))
            except FileNotFoundError:
                continue
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    