from abc import ABC, abstractmethod
from typing import Optional

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
