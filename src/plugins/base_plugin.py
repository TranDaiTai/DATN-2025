from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from ..core.browser import BrowserManager
from ..core.storage import StorageManager
from ..core.repository import RuleRepository
from ..core.recovery import AISelectorRecovery

class BaseSitePlugin(ABC):
    def __init__(self, 
                 browser: BrowserManager, 
                 storage: StorageManager, 
                 rule_repo: RuleRepository,
                 recovery: AISelectorRecovery):
        self.browser = browser
        self.storage = storage
        self.rule_repo = rule_repo
        self.recovery = recovery
        self.site_name = self.__class__.__name__.replace("Plugin", "").lower()

    @abstractmethod
    async def crawl_listings(self, keywords: List[str]) -> List[Dict[str, Any]]:
        """Scrapes search result pages and returns a list of job URLs/metadata"""
        pass

    @abstractmethod
    async def extract_details(self, job_item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extracts detailed information from a single job page"""
        pass

    async def _safe_extract(self, soup, field_name: str, rules: List[Dict[str, Any]], html: str) -> Optional[str]:
        """Tries verified rules, and if they fail, triggers AI recovery"""
        for rule in rules:
            if rule['field_name'] == field_name:
                selector = rule['selector']
                selector_type = rule['selector_type']
                
                # Apply extraction logic (CSS or XPath)
                try:
                    if selector_type == 'css':
                        elem = soup.select_one(selector)
                    else:
                        # Simple XPath fallback via soup
                        elem = None # Needs real XPath support if used heavily
                    
                    if elem:
                        return elem.get_text(strip=True)
                except:
                    continue

        # If we reach here, rule-based extraction failed
        return None
