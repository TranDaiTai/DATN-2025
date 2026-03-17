import aioboto3
import os
from typing import Optional
from ..utils.logger import logger

class S3StorageAdapter:
    """
    Adapter để lưu trữ file lên AWS S3 hoặc các dịch vụ tương đương (Cloudflare R2, DigitalOcean).
    Giúp lưu trữ HTML thô lên Cloud thay vì ổ cứng cục bộ.
    """
    def __init__(self, 
                 bucket_name: str, 
                 access_key: str, 
                 secret_key: str, 
                 region: str = "us-east-1",
                 endpoint_url: Optional[str] = None):
        self.bucket = bucket_name
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region
        self.endpoint_url = endpoint_url
        self.session = aioboto3.Session()

    async def save_html(self, html: str, filename: str) -> str:
        """Upload nội dung HTML lên S3 Bucket"""
        logger.info("s3_adapter.upload.start", bucket=self.bucket, filename=filename)
        try:
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
            logger.info("s3_adapter.upload.success", filename=filename)
            return f"s3://{self.bucket}/html/{filename}"
        except Exception as e:
            logger.error("s3_adapter.upload.failed", error=str(e))
            raise
