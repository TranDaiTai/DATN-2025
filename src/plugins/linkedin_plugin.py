from .base_plugin import BaseSitePlugin
from typing import List, Dict, Any, Optional
from bs4 import BeautifulSoup
import re
from ..utils.logger import logger
from ..utils.exceptions import BrowserTimeoutError, CaptchaDetectedError

class LinkedinPlugin(BaseSitePlugin):
    """
    Plugin dành riêng cho LinkedIn.
    Sử dụng Guest API để tìm danh sách và bóc tách chi tiết Job.
    """
    async def crawl_listings(self, keywords: List[str]) -> List[Dict[str, Any]]:
        # URL tìm kiếm ẩn danh của LinkedIn
        list_url = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
        job_items = []

        for kw in keywords:
            logger.info("linkedin.crawl_listings.start", keyword=kw)
            params = f"?keywords={kw}&location=Vietnam&start=0"
            try:
                html = await self.browser.fetch_page_html(list_url + params)
                soup = BeautifulSoup(html, "html.parser")
                cards = soup.find_all("li")
                
                for card in cards:
                    link = card.find("a", class_=re.compile(r"base-card__full-link|result-card__full-link"))
                    if not link or "href" not in link.attrs: continue
                    
                    url = link["href"].split("?")[0]
                    # Trích xuất Job ID từ URL bằng Regex
                    job_id_match = re.search(r"-(\d+)", url) or re.search(r"/view/(\d+)", url)
                    if job_id_match:
                        job_items.append({
                            "job_id": job_id_match.group(1),
                            "url": url,
                            "site_name": "linkedin"
                        })
                logger.info("linkedin.crawl_listings.complete", keyword=kw, found=len(job_items))
            except (BrowserTimeoutError, CaptchaDetectedError) as e:
                logger.error("linkedin.crawl_listings.failed", keyword=kw, error=str(e))
                continue
        
        return job_items

    async def extract_details(self, job_item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Bóc tách thông tin chi tiết từ trang Job đơn lẻ"""
        url = job_item["url"]
        detail_url = f"https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_item['job_id']}"
        
        try:
            html = await self.browser.fetch_page_html(detail_url)
            await self.storage.save_html(html, url) # Lưu HTML thô phục vụ bằng chứng đồ án
            
            soup = BeautifulSoup(html, "html.parser")
            # Sử dụng Repository Pattern để lấy Rule từ Database
            rules = await self.rule_repo.get_active_rules("linkedin")
            
            # Trích xuất các trường cơ bản
            title = await self._safe_extract(soup, "job_title", rules, html)
            company = await self._safe_extract(soup, "company_name", rules, html)
            description = await self._safe_extract(soup, "description", rules, html)

            # Cơ chế Tự chữa lành: Nếu không lấy được Title (trường quan trọng nhất)
            if not title:
                logger.warning("linkedin.extract.rule_failed", field="job_title", url=url)
                recovery_result = await self.recovery.recover_selector("linkedin", "job_title", html)
                if recovery_result:
                    selector = recovery_result['selector']
                    found_elem = soup.select_one(selector)
                    if found_elem:
                        title = found_elem.get_text(strip=True)
                        # Lưu Selector mới vào DB thông qua Repository
                        await self.rule_repo.create_candidate(
                            "linkedin", "job_title", selector, recovery_result['selector_type'], "ai_recovery"
                        )

            return {
                "job_title": title or "N/A",
                "company_name": company or "N/A",
                "description": description or "N/A",
                "url": url,
                "site_name": "linkedin"
            }
        except Exception as e:
            logger.error("linkedin.extract.failed", url=url, error=str(e))
            return None
