import hashlib
import datetime
import aioboto3
from typing import Dict, Any, Optional
from ...interfaces.core_interfaces import StorageStrategy
from ....utils.logger import logger

class S3StorageStrategy(StorageStrategy):
    def __init__(self, bucket_name: str, access_key: str, secret_key: str, region: str = "us-east-1", endpoint_url: Optional[str] = None):
        self.bucket = bucket_name
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region
        self.endpoint_url = endpoint_url
        self.session = aioboto3.Session()

    async def save_html(self, html: str, url: str) -> str:
        url_hash = hashlib.md5(url.encode()).hexdigest()
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{url_hash}_{timestamp}.html"
        
        logger.info("storage.s3.upload_start", bucket=self.bucket, filename=filename)
        async with self.session.client(
            's3',
            region_name=self.region,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            endpoint_url=self.endpoint_url
        ) as s3:
            await s3.put_object(
                Bucket=self.bucket,
                Key=f"html/{filename}",
                Body=html.encode('utf-8'),
                ContentType='text/html'
            )
        logger.info("storage.s3.upload_success", filename=filename)
        return f"s3://{self.bucket}/html/{filename}"

    async def save_json(self, data: Dict[str, Any], filename: str):
        logger.warning("storage.s3.save_json_not_implemented")
        pass
