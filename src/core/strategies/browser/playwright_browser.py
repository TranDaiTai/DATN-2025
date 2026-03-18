import asyncio
import os
from playwright.async_api import async_playwright
from playwright_stealth import Stealth
from typing import Optional
from ...interfaces.core_interfaces import BrowserStrategy
from ....adapters.proxy_adapter import ProxyAdapter
from ....utils.exceptions import BrowserTimeoutError, CaptchaDetectedError
from ....utils.logger import logger

class PlaywrightBrowserStrategy(BrowserStrategy):
    def __init__(self, proxy_adapter: ProxyAdapter):
        self.proxy_adapter = proxy_adapter
        self.playwright = None
        self.browser = None
        self.context = None
        self.headless = os.getenv("HEADLESS", "True").lower() == "true"
        self.timeout = int(os.getenv("REQUEST_TIMEOUT", 30)) * 1000

    async def start(self):
        logger.info("browser.playwright.start", headless=self.headless)
        self.playwright = await async_playwright().start()
        proxy = self.proxy_adapter.get_proxy()
        
        self.browser = await self.playwright.chromium.launch(
            headless=self.headless,
            proxy=proxy
        )
        self.context = await self.browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )

    async def _get_page(self):
        page = await self.context.new_page()
        await Stealth().apply_stealth_async(page)
        return page

    async def close(self):
        logger.info("browser.playwright.close")
        if self.context: await self.context.close()
        if self.browser: await self.browser.close()
        if self.playwright: await self.playwright.stop()

    async def fetch_page_html(self, url: str, wait_selector: Optional[str] = None) -> str:
        page = await self._get_page()
        try:
            logger.info("browser.playwright.fetch_url", url=url)
            await page.goto(url, wait_until="networkidle", timeout=self.timeout)
            
            content = await page.content()
            if "captcha" in content.lower() or "security check" in content.lower():
                logger.error("browser.playwright.captcha_detected", url=url)
                raise CaptchaDetectedError(f"CAPTCHA detected at {url}")
            
            if wait_selector:
                try:
                    await page.wait_for_selector(wait_selector, timeout=5000)
                except Exception:
                    logger.warning("browser.playwright.wait_selector_timeout", selector=wait_selector)
            
            return await page.content()
        except asyncio.TimeoutError:
            logger.error("browser.playwright.timeout", url=url)
            raise BrowserTimeoutError(f"Timeout while loading {url}")
        finally:
            await page.close()
