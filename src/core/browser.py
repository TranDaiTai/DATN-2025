from typing import Optional
from .interfaces.core_interfaces import BrowserStrategy

class BrowserManager:
    """
    Context class in Strategy Pattern.
    Delegates browser operations to a specific BrowserStrategy.
    """
    def __init__(self, strategy: BrowserStrategy):
        self.strategy = strategy

    async def start(self):
        await self.strategy.start()

    async def close(self):
        await self.strategy.close()

    async def fetch_page_html(self, url: str, wait_selector: Optional[str] = None) -> str:
        return await self.strategy.fetch_page_html(url, wait_selector)
