import pandas as pd
import os

from datetime import datetime
from s3_cache.s3_cache import FileSystem


class DataStream:
    def __init__(self, stream_name: str, base_folder: str, file_system: FileSystem, prefix: str = "", suffix: str = ""):
        self.stream_name = stream_name
        self.base_folder = base_folder
        self.prefix = prefix
        self.suffix = suffix
        self.file_system = file_system

    def get_data_for_date(self, date):
        """Retrieve data for a single date as DataFrame using the file_system."""
        file_path = self._build_file_path(date)
        try:
            resolved_path = self.file_system.get_file(file_path)
            return pd.read_csv(resolved_path)
        except FileNotFoundError:
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
        return self.file_system.get_file(self.base_folder, filename)

    def _load_multiple_files(self, dates):
        """Load and combine multiple CSV files using file_system for file lookup"""
        dfs = []
        for date in dates:
            file_path = self._build_file_path(date)
            try:
                resolved_path = self.file_system.get_file(file_path)
                dfs.append(pd.read_csv(resolved_path))
            except FileNotFoundError:
                continue
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

