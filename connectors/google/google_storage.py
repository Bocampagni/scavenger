"""
Google Cloud Storage Connector

This module provides a connector to interact with Google Cloud Storage (GCS).
It supports uploading, downloading, and listing objects in GCS buckets.
"""

import logging
from typing import Optional, Dict, Any
from datetime import datetime

try:
    from google.cloud import storage
    from google.oauth2 import service_account
    GCS_AVAILABLE = True
except ImportError:
    GCS_AVAILABLE = False
    storage = None
    service_account = None

from config import Config

logger = logging.getLogger(__name__)


class GoogleStorageConnector:
    """
    A connector class for Google Cloud Storage operations.
    
    This class provides methods to interact with GCS buckets including:
    - Uploading files/data to buckets
    - Downloading files/data from buckets  
    - Listing objects in buckets
    """
    
    def __init__(self, project_id: Optional[str] = None, credentials_path: Optional[str] = None):
        """
        Initialize the Google Storage connector.
        
        Args:
            project_id: GCP project ID. If None, will be inferred from credentials.
            credentials_path: Path to service account JSON file. If None, uses GCP_API_KEY from config.
        """
        if not GCS_AVAILABLE:
            raise ImportError(
                "Google Cloud Storage dependencies not installed. "
                "Install with: pip install google-cloud-storage"
            )
        
        self.project_id = project_id
        self.credentials_path = credentials_path
        self._client = None
        
    @property
    def client(self):
        """
        Lazy initialization of the GCS client.
        
        Returns:
            storage.Client: Authenticated GCS client
        """
        if self._client is None:
            try:
                if self.credentials_path:
                    # Use service account file
                    credentials = service_account.Credentials.from_service_account_file(
                        self.credentials_path
                    )
                    self._client = storage.Client(
                        project=self.project_id,
                        credentials=credentials
                    )
                elif Config.GCP_API_KEY:
                    # Use API key or service account JSON from config
                    # Note: For production, use service account JSON file
                    logger.warning("Using GCP_API_KEY from config. Consider using service account file for production.")
                    # This assumes GCP_API_KEY contains the service account JSON content
                    import json
                    credentials_info = json.loads(Config.GCP_API_KEY)
                    credentials = service_account.Credentials.from_service_account_info(credentials_info)
                    self._client = storage.Client(
                        project=self.project_id or credentials_info.get('project_id'),
                        credentials=credentials
                    )
                else:
                    # Use default credentials (ADC - Application Default Credentials)
                    self._client = storage.Client(project=self.project_id)
                    
            except Exception as e:
                logger.error(f"Failed to initialize GCS client: {e}")
                raise
                
        return self._client
    
    def upload_file(
        self, 
        bucket_name: str, 
        file_path: str, 
        destination_blob_name: str,
        metadata: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Upload a file to a Google Cloud Storage bucket.
        
        Args:
            bucket_name: Name of the GCS bucket
            file_path: Local path to the file to upload
            destination_blob_name: Name for the object in GCS
            metadata: Optional metadata to attach to the object
            
        Returns:
            Dict containing upload information (blob name, size, creation time, etc.)
            
        Raises:
            Exception: If upload fails
        """
        try:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)
            
            # Set metadata if provided
            if metadata:
                blob.metadata = metadata
            
            # Upload the file
            with open(file_path, 'rb') as file_obj:
                blob.upload_from_file(file_obj)
            
            logger.info(f"Successfully uploaded {file_path} to gs://{bucket_name}/{destination_blob_name}")
            
            return {
                'bucket_name': bucket_name,
                'blob_name': destination_blob_name,
                'size': blob.size,
                'created': blob.time_created.isoformat() if blob.time_created else None,
                'updated': blob.updated.isoformat() if blob.updated else None,
                'md5_hash': blob.md5_hash,
                'public_url': f"gs://{bucket_name}/{destination_blob_name}",
                'metadata': blob.metadata
            }
            
        except Exception as e:
            logger.error(f"Failed to upload file {file_path} to GCS: {e}")
            raise
    
    def upload_data(
        self,
        bucket_name: str,
        data: str,
        destination_blob_name: str,
        content_type: str = "text/plain",
        metadata: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Upload string data directly to a Google Cloud Storage bucket.
        
        Args:
            bucket_name: Name of the GCS bucket
            data: String data to upload
            destination_blob_name: Name for the object in GCS
            content_type: MIME type of the data
            metadata: Optional metadata to attach to the object
            
        Returns:
            Dict containing upload information
            
        Raises:
            Exception: If upload fails
        """
        try:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)
            
            # Set content type and metadata
            blob.content_type = content_type
            if metadata:
                blob.metadata = metadata
            
            # Upload the data
            blob.upload_from_string(data)
            
            logger.info(f"Successfully uploaded data to gs://{bucket_name}/{destination_blob_name}")
            
            return {
                'bucket_name': bucket_name,
                'blob_name': destination_blob_name,
                'size': blob.size,
                'content_type': blob.content_type,
                'created': blob.time_created.isoformat() if blob.time_created else None,
                'updated': blob.updated.isoformat() if blob.updated else None,
                'md5_hash': blob.md5_hash,
                'public_url': f"gs://{bucket_name}/{destination_blob_name}",
                'metadata': blob.metadata
            }
            
        except Exception as e:
            logger.error(f"Failed to upload data to GCS: {e}")
            raise
    
    def download_file(
        self, 
        bucket_name: str, 
        source_blob_name: str, 
        destination_file_path: str
    ) -> Dict[str, Any]:
        """
        Download a file from Google Cloud Storage to local filesystem.
        
        Args:
            bucket_name: Name of the GCS bucket
            source_blob_name: Name of the object in GCS
            destination_file_path: Local path where file will be saved
            
        Returns:
            Dict containing download information
            
        Raises:
            Exception: If download fails
        """
        try:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(source_blob_name)
            
            # Check if blob exists
            if not blob.exists():
                raise FileNotFoundError(f"Object gs://{bucket_name}/{source_blob_name} not found")
            
            # Download the file
            blob.download_to_filename(destination_file_path)
            
            logger.info(f"Successfully downloaded gs://{bucket_name}/{source_blob_name} to {destination_file_path}")
            
            return {
                'bucket_name': bucket_name,
                'blob_name': source_blob_name,
                'local_path': destination_file_path,
                'size': blob.size,
                'content_type': blob.content_type,
                'created': blob.time_created.isoformat() if blob.time_created else None,
                'updated': blob.updated.isoformat() if blob.updated else None,
                'md5_hash': blob.md5_hash,
                'metadata': blob.metadata
            }
            
        except Exception as e:
            logger.error(f"Failed to download file from GCS: {e}")
            raise
    
    def download_as_text(
        self, 
        bucket_name: str, 
        source_blob_name: str,
        encoding: str = 'utf-8'
    ) -> Dict[str, Any]:
        """
        Download a file from Google Cloud Storage as text content.
        
        Args:
            bucket_name: Name of the GCS bucket
            source_blob_name: Name of the object in GCS
            encoding: Text encoding to use for decoding
            
        Returns:
            Dict containing the text content and file information
            
        Raises:
            Exception: If download fails
        """
        try:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(source_blob_name)
            
            # Check if blob exists
            if not blob.exists():
                raise FileNotFoundError(f"Object gs://{bucket_name}/{source_blob_name} not found")
            
            # Download as text
            content = blob.download_as_text(encoding=encoding)
            
            logger.info(f"Successfully downloaded gs://{bucket_name}/{source_blob_name} as text")
            
            return {
                'bucket_name': bucket_name,
                'blob_name': source_blob_name,
                'content': content,
                'size': blob.size,
                'content_type': blob.content_type,
                'encoding': encoding,
                'created': blob.time_created.isoformat() if blob.time_created else None,
                'updated': blob.updated.isoformat() if blob.updated else None,
                'md5_hash': blob.md5_hash,
                'metadata': blob.metadata
            }
            
        except Exception as e:
            logger.error(f"Failed to download text from GCS: {e}")
            raise
    
    def list_objects(
        self, 
        bucket_name: str, 
        prefix: Optional[str] = None,
        max_results: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        List objects in a Google Cloud Storage bucket.
        
        Args:
            bucket_name: Name of the GCS bucket
            prefix: Only list objects with this prefix
            max_results: Maximum number of objects to return
            
        Returns:
            Dict containing list of objects and bucket information
            
        Raises:
            Exception: If listing fails
        """
        try:
            bucket = self.client.bucket(bucket_name)
            
            # List objects with optional prefix filter
            blobs = bucket.list_blobs(prefix=prefix, max_results=max_results)
            
            objects = []
            for blob in blobs:
                objects.append({
                    'name': blob.name,
                    'size': blob.size,
                    'content_type': blob.content_type,
                    'created': blob.time_created.isoformat() if blob.time_created else None,
                    'updated': blob.updated.isoformat() if blob.updated else None,
                    'md5_hash': blob.md5_hash,
                    'public_url': f"gs://{bucket_name}/{blob.name}",
                    'metadata': blob.metadata
                })
            
            logger.info(f"Successfully listed {len(objects)} objects from gs://{bucket_name}")
            
            return {
                'bucket_name': bucket_name,
                'prefix': prefix,
                'object_count': len(objects),
                'objects': objects,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to list objects from GCS bucket {bucket_name}: {e}")
            raise
    
    def object_exists(self, bucket_name: str, blob_name: str) -> bool:
        """
        Check if an object exists in the bucket.
        
        Args:
            bucket_name: Name of the GCS bucket
            blob_name: Name of the object to check
            
        Returns:
            True if object exists, False otherwise
        """
        try:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            return blob.exists()
        except Exception as e:
            logger.error(f"Failed to check if object exists: {e}")
            return False
    
    def delete_object(self, bucket_name: str, blob_name: str) -> bool:
        """
        Delete an object from the bucket.
        
        Args:
            bucket_name: Name of the GCS bucket
            blob_name: Name of the object to delete
            
        Returns:
            True if deletion was successful, False otherwise
        """
        try:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            
            if blob.exists():
                blob.delete()
                logger.info(f"Successfully deleted gs://{bucket_name}/{blob_name}")
                return True
            else:
                logger.warning(f"Object gs://{bucket_name}/{blob_name} does not exist")
                return False
                
        except Exception as e:
            logger.error(f"Failed to delete object gs://{bucket_name}/{blob_name}: {e}")
            return False


# Convenience functions for direct usage
def upload_file_to_gcs(
    bucket_name: str,
    file_path: str,
    destination_blob_name: str,
    metadata: Optional[Dict[str, str]] = None,
    project_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Convenience function to upload a file to GCS.
    
    Args:
        bucket_name: Name of the GCS bucket
        file_path: Local path to the file to upload
        destination_blob_name: Name for the object in GCS
        metadata: Optional metadata to attach to the object
        project_id: GCP project ID
        
    Returns:
        Dict containing upload information
    """
    connector = GoogleStorageConnector(project_id=project_id)
    return connector.upload_file(bucket_name, file_path, destination_blob_name, metadata)


def download_file_from_gcs(
    bucket_name: str,
    source_blob_name: str,
    destination_file_path: str,
    project_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Convenience function to download a file from GCS.
    
    Args:
        bucket_name: Name of the GCS bucket
        source_blob_name: Name of the object in GCS
        destination_file_path: Local path where file will be saved
        project_id: GCP project ID
        
    Returns:
        Dict containing download information
    """
    connector = GoogleStorageConnector(project_id=project_id)
    return connector.download_file(bucket_name, source_blob_name, destination_file_path)


def list_gcs_objects(
    bucket_name: str,
    prefix: Optional[str] = None,
    max_results: Optional[int] = None,
    project_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Convenience function to list objects in a GCS bucket.
    
    Args:
        bucket_name: Name of the GCS bucket
        prefix: Only list objects with this prefix
        max_results: Maximum number of objects to return
        project_id: GCP project ID
        
    Returns:
        Dict containing list of objects and bucket information
    """
    connector = GoogleStorageConnector(project_id=project_id)
    return connector.list_objects(bucket_name, prefix, max_results)
