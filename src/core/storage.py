from typing import Dict, Any, Optional
from .interfaces.core_interfaces import StorageStrategy

class StorageManager:
    """
    Context class in Strategy Pattern.
    Delegates storage operations to a specific StorageStrategy.
    """
    def __init__(self, strategy: StorageStrategy):
        self.strategy = strategy

    async def save_html(self, html: str, url: str) -> str:
        """Lưu HTML thô thông qua chiến lược đã chọn"""
        return await self.strategy.save_html(html, url)

    async def save_job_json(self, job_data: Dict[str, Any], filename: str = "jobs.jsonl"):
        """Lưu kết quả trích xuất vào file JSON thông qua chiến lược đã chọn"""
        await self.strategy.save_json(job_data, filename)
