from ..strategies.storage.local_storage import LocalStorageStrategy
from ..strategies.storage.s3_storage import S3StorageStrategy
from ..storage import StorageManager
from ..config import settings

class StorageFactory:
    @staticmethod
    def create_storage() -> StorageManager:
        if settings.S3_BUCKET:
            strategy = S3StorageStrategy(
                bucket_name=settings.S3_BUCKET,
                access_key=settings.S3_ACCESS_KEY,
                secret_key=settings.S3_SECRET_KEY,
                region=settings.S3_REGION,
                endpoint_url=settings.S3_ENDPOINT
            )
        else:
            strategy = LocalStorageStrategy()
            
        return StorageManager(strategy)
