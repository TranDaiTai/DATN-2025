from abc import ABC, abstractmethod
from typing import Dict, Any, Optional

class StorageStrategy(ABC):
    @abstractmethod
    async def save_html(self, html: str, url: str) -> str:
        """Lưu trữ HTML thô"""
        pass

    @abstractmethod
    async def save_json(self, data: Dict[str, Any], filename: str):
        """Lưu trữ JSON"""
        pass

class BrowserStrategy(ABC):
    @abstractmethod
    async def start(self):
        """Khởi động trình duyệt"""
        pass

    @abstractmethod
    async def close(self):
        """Đóng toàn bộ tài nguyên trình duyệt"""
        pass

    @abstractmethod
    async def fetch_page_html(self, url: str, wait_selector: Optional[str] = None) -> str:
        """Truy cập URL và lấy nội dung HTML thô"""
        pass
