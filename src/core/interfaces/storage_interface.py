from abc import ABC, abstractmethod
from typing import Dict, Any, Optional

class StorageStrategy(ABC):
    @abstractmethod
    async def save_html(self, html: str, url: str) -> str:
        """Lưu trữ HTML thô"""
        pass

    @abstractmethod
    async def save_json(self, data: Dict[str, Any], filename: str):
        """Lưu trữ JSON (thường là local kết quả)"""
        pass
