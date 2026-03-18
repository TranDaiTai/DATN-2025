import os
from ..strategies.browser.playwright_browser import PlaywrightBrowserStrategy
from ..browser import BrowserManager
from ..config import settings
from ...adapters.proxy_adapter import ProxyAdapter

class BrowserFactory:
    @staticmethod
    def create_browser(proxy_adapter: ProxyAdapter, browser_type: str = "playwright") -> BrowserManager:
        if browser_type.lower() == "playwright":
            strategy = PlaywrightBrowserStrategy(proxy_adapter)
        else:
            # Ở đây có thể thêm các strategy khác như Selenium, Requests trong tương lai
            raise ValueError(f"Browser type {browser_type} not supported")
            
        return BrowserManager(strategy)
