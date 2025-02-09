import os
import boto3
from botocore.exceptions import ClientError
from datetime import datetime
import logging
import pandas as pd
import os
from datetime import datetime
from abc import ABC, abstractmethod
import glob


class FileSystem(ABC):
    """Abstract base class for file system operations"""
    
    @abstractmethod
    def get_file(self, path: str, *params) -> str:
        """Retrieve file from the filesystem"""
        pass
    
    @abstractmethod
    def modified(self, path: str, *params) -> datetime:
        """Get last modified timestamp of file"""
        pass
        
    @abstractmethod
    def ls(self, path: str, *params) -> list[str]:
        """List contents of directory"""
        pass
        
    @abstractmethod
    def glob(self, pattern: str) -> list[str]:
        """Find files matching pattern"""
        pass
        
    @abstractmethod
    def created(self, path: str, *params) -> datetime:
        """Get creation timestamp of file"""
        pass

    def _create_full_path(self, path: str, params: list[str]) -> str:
        if not params:
            return path
        return f"{path}/{'/'.join(params)}"




class S3FileSystem(FileSystem):
    def __init__(self, bucket_name, cache_dir='s3_cache'):
        self.bucket_name = bucket_name
        self.cache_dir = cache_dir
        self.s3 = boto3.client('s3')
        
        # Create cache directory if it doesn't exist
        os.makedirs(self.cache_dir, exist_ok=True)

    def get_file(self, path, *params):
        """Retrieve file from cache or S3, returns local file path"""
        full_path = self._create_full_path(path, list(params))
        local_path = os.path.join(self.cache_dir, full_path.replace('/', os.sep))
        metadata_file = f"{local_path}.meta"

        # Check if file exists in cache
        if os.path.exists(local_path):
            # Verify if cache is stale
            if self._is_cache_valid(full_path, metadata_file):
                return local_path
                
        # Download from S3 if cache is invalid/missing
        return self._refresh_cache(full_path, local_path, metadata_file)

    def modified(self, path: str, *params) -> datetime:
        """Get the last modified timestamp of the S3 object."""
        full_path = self._create_full_path(path, list(params))
        fs = S3FileSystem()
        return fs.modified(path=full_path)


    def ls(self, path: str, *params) -> list[str]:
        """List contents of the S3 bucket under the given prefix."""
        full_path = self._create_full_path(path, list(params))
        fs = S3FileSystem()
        return fs.ls(path=full_path)


    def glob(self, pattern: str) -> list[str]:
        """Find S3 objects matching the given glob pattern."""
        fs = S3FileSystem()
        return fs.glob(pattern=pattern)
        
    def created(self, path: str, *params) -> datetime:
        """Get the creation timestamp of the S3 object (using LastModified as a proxy)."""
        full_path = self._create_full_path(path, list(params))
        fs = S3FileSystem()
        return fs.created(path=full_path)


    def _is_cache_valid(self, full_path: str, metadata_file: str) -> bool:
        """Check if cached file matches S3's Last-Modified"""
        try:
            head = self.s3.head_object(Bucket=self.bucket_name, Key=full_path)
            s3_last_modified = head['LastModified'].timestamp()
            
            # Get cached metadata
            if os.path.exists(metadata_file):
                with open(metadata_file, 'r') as f:
                    cached_last_modified = float(f.read())
                    return s3_last_modified <= cached_last_modified
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                raise FileNotFoundError(f"Object {full_path} not found in S3")
            raise
        return False

    def _refresh_cache(self, full_path: str, local_path: str, metadata_file: str):
        """Download file from S3 and update cache"""
        try:
            # Download file

            self.s3.download_file(self.bucket_name, full_path, local_path)
            
            # Get and store last modified timestamp
            head = self.s3.head_object(Bucket=self.bucket_name, Key=full_path)
            last_modified = head['LastModified'].timestamp()
            
            with open(metadata_file, 'w') as f:
                f.write(str(last_modified))
                
            return local_path
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                raise FileNotFoundError(f"Object {full_path} not found in S3")
            raise



class LocalFileSystem(FileSystem):

    def __init__(self, base_dir=""):
        self.base_dir = base_dir

    def get_file(self, path: str) -> str:
        path = path.replace('/', os.sep)
        return os.path.join(self.base_dir, path)
        
    def modified(self, path: str) -> datetime:
        return datetime.fromtimestamp(os.path.getmtime(self.get_file(path)))
        
    def ls(self, path: str) -> list[str]:
        return os.listdir(self.get_file(path))
        
    def glob(self, pattern: str) -> list[str]:
        return glob.glob(self.get_file(pattern))
        
    def created(self, path: str) -> datetime:
        return datetime.fromtimestamp(os.path.getctime(self.get_file(path)))


# Usage example
if __name__ == "__main__":
    # Test configuration
    bucket_name = 'testbucket'
    test_key = 'EOD_Prices/EOD_Prices_2025-01-03.csv'
    
    # Initialize cache
    cache = S3FileSystem(bucket_name)
    print(f"Initialized cache for bucket: {bucket_name}")
    
    # Attempt to get file
    local_file = cache.get_file(test_key)
    print(f"Retrieved file: {local_file}")

