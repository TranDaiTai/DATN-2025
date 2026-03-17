import os
import json
import hashlib
import datetime
from typing import Dict, Any, Optional
from ..utils.logger import logger

class StorageManager:
    """
    Quản lý việc lưu trữ dữ liệu (HTML thô và kết quả JSON).
    Hỗ trợ cả lưu trữ Cục bộ (Local) và Cloud (S3) thông qua cấu hình.
    """
    def __init__(self, base_dir: str = "data", s_3_adapter: Optional[Any] = None):
        self.base_dir = base_dir
        self.html_dir = os.path.join(base_dir, "html")
        self.output_dir = os.path.join(base_dir, "output")
        self.s3_adapter = s_3_adapter # Adapter cho S3 nếu có
        
        # Đảm bảo các thư mục tồn tại nếu lưu Local
        os.makedirs(self.html_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)

    async def save_html(self, html: str, url: str) -> str:
        """Lưu HTML thô (Cục bộ hoặc S3 tùy cấu hình)"""
        url_hash = hashlib.md5(url.encode()).hexdigest()
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{url_hash}_{timestamp}.html"
        
        # Ưu tiên lưu lên S3 nếu Adapter được cung cấp
        if self.s3_adapter:
            return await self.s3_adapter.save_html(html, filename)
        
        # Nếu không, lưu cục bộ vào ổ cứng
        filepath = os.path.join(self.html_dir, filename)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(html)
        
        logger.info("storage.save_local", filename=filename)
        return filename

    def save_job_json(self, job_data: Dict[str, Any], filename: str = "jobs.jsonl"):
        """Lưu kết quả trích xuất vào file JSONL (Local)"""
        filepath = os.path.join(self.output_dir, filename)
        with open(filepath, "a", encoding="utf-8") as f:
            f.write(json.dumps(job_data, ensure_ascii=False) + "\n")
