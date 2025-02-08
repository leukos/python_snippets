import os
import boto3
from botocore.exceptions import ClientError
from datetime import datetime
import logging
import pandas as pd
import os
from datetime import datetime

class S3FileCache:
    def __init__(self, bucket_name, cache_dir='s3_cache'):
        self.bucket_name = bucket_name
        self.cache_dir = cache_dir
        self.s3 = boto3.client('s3')
        
        # Create cache directory if it doesn't exist
        os.makedirs(self.cache_dir, exist_ok=True)

    def get_file(self, s3_key):
        """Retrieve file from cache or S3, returns local file path"""
        local_path = os.path.join(self.cache_dir, s3_key.replace('/', '_'))
        metadata_file = f"{local_path}.meta"

        # Check if file exists in cache
        if os.path.exists(local_path):
            # Verify if cache is stale
            if self._is_cache_valid(s3_key, metadata_file):
                print(f'Using local file {local_path}')
                return local_path
                
        # Download from S3 if cache is invalid/missing
        return self._refresh_cache(s3_key, local_path, metadata_file)

    def _is_cache_valid(self, s3_key, metadata_file):
        """Check if cached file matches S3's Last-Modified"""
        try:
            # Get S3 object metadata
            head = self.s3.head_object(Bucket=self.bucket_name, Key=s3_key)
            s3_last_modified = head['LastModified'].timestamp()
            
            # Get cached metadata
            if os.path.exists(metadata_file):
                with open(metadata_file, 'r') as f:
                    cached_last_modified = float(f.read())
                    return s3_last_modified <= cached_last_modified
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                raise FileNotFoundError(f"Object {s3_key} not found in S3")
            raise
        return False

    def _refresh_cache(self, s3_key, local_path, metadata_file):
        """Download file from S3 and update cache"""
        try:
            # Download file
            self.s3.download_file(self.bucket_name, s3_key, local_path)
            
            # Get and store last modified timestamp
            head = self.s3.head_object(Bucket=self.bucket_name, Key=s3_key)
            last_modified = head['LastModified'].timestamp()
            
            with open(metadata_file, 'w') as f:
                f.write(str(last_modified))
                
            return local_path
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                raise FileNotFoundError(f"Object {s3_key} not found in S3")
            raise



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
    

import pandas as pd
import os
from datetime import datetime

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


            
# Usage example
if __name__ == "__main__":
    cache = S3FileCache('testbucket')
    
    # This will download and cache the file
    local_file = cache.get_file('EOD_Prices/EOD_Prices_2025-01-03.csv')
    print(f"Using cached file at: {local_file}")
    
    # Subsequent calls will use cached version unless S3 object changes
    local_file = cache.get_file('EOD_Prices/EOD_Prices_2025-01-02.csv')
