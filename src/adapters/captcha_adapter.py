from abc import ABC, abstractmethod
import logging

logger = logging.getLogger(__name__)

class CaptchaAdapter(ABC):
    @abstractmethod
    async def solve(self, site_key: str, url: str) -> str:
        pass

class ManualNoticeCaptcha(CaptchaAdapter):
    async def solve(self, site_key: str, url: str) -> str:
        logger.warning(f"CAPTCHA detected at {url}. Manual intervention required (No solver configured).")
        return "" # Return empty, logic will handle failure

class TwoCaptchaAdapter(CaptchaAdapter):
    def __init__(self, api_key: str):
        self.api_key = api_key

    async def solve(self, site_key: str, url: str) -> str:
        # Placeholder for 2captcha implementation
        logger.info(f"Solving CAPTCHA at {url} using 2captcha...")
        return "solved_token"
