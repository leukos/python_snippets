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
    def get_file(self, path: str) -> str:
        """Retrieve file from the filesystem"""
        pass
    
    @abstractmethod
    def modified(self, path: str) -> datetime:
        """Get last modified timestamp of file"""
        pass
        
    @abstractmethod
    def ls(self, path: str) -> list[str]:
        """List contents of directory"""
        pass
        
    @abstractmethod
    def glob(self, pattern: str) -> list[str]:
        """Find files matching pattern"""
        pass
        
    @abstractmethod
    def created(self, path: str) -> datetime:
        """Get creation timestamp of file"""
        pass


class S3FileSystem(FileSystem):
    def __init__(self, bucket_name, cache_dir='s3_cache'):
        self.bucket_name = bucket_name
        self.cache_dir = cache_dir
        self.s3 = boto3.client('s3')
        
        # Create cache directory if it doesn't exist
        os.makedirs(self.cache_dir, exist_ok=True)

    def get_file(self, s3_key):
        """Retrieve file from cache or S3, returns local file path"""
        local_path = os.path.join(self.cache_dir, s3_key.replace('/', os.sep))
        metadata_file = f"{local_path}.meta"

        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        # Check if file exists in cache
        if os.path.exists(local_path):
            # Verify if cache is stale
            if self._is_cache_valid(s3_key, metadata_file):
                return local_path
                
        # Download from S3 if cache is invalid/missing
        return self._refresh_cache(s3_key, local_path, metadata_file)

    def modified(self, path: str) -> datetime:
        """Get the last modified timestamp of the S3 object."""
        fs = S3FileSystem()
        return fs.modified(path=path)

    def ls(self, path: str) -> list[str]:
        """List contents of the S3 bucket under the given prefix."""
        fs = S3FileSystem()
        return fs.ls(path=path)

    def glob(self, pattern: str) -> list[str]:
        """Find S3 objects matching the given glob pattern."""
        fs = S3FileSystem()
        return fs.glob(pattern=pattern)
        
    def created(self, path: str) -> datetime:
        """Get the creation timestamp of the S3 object (using LastModified as a proxy)."""
        fs = S3FileSystem()
        return fs.created(path=path)

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

