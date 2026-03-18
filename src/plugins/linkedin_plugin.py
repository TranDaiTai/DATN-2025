from .base_plugin import BaseSitePlugin
from typing import List, Dict, Any, Optional
from bs4 import BeautifulSoup
import re
from ..utils.logger import logger
from ..utils.exceptions import BrowserTimeoutError, CaptchaDetectedError
import datetime

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
                logger.info("linkedin.crawl_listings.fetch_success", keyword=kw, html_len=len(html))
                
                # Debug: Lưu HTML trang danh sách
                debug_filename = f"linkedin_list_{kw}.html"
                with open(f"data/html/{debug_filename}", "w", encoding="utf-8") as f:
                    f.write(html)

                soup = BeautifulSoup(html, "html.parser")
                cards = soup.find_all("li")
                logger.info("linkedin.crawl_listings.parse", keyword=kw, cards_count=len(cards))
                
                for card in cards:
                    # Cập nhật regex cho link phù hợp với UI mới
                    link = card.find("a", class_=re.compile(r"base-card__full-link|result-card__full-link|base-card-relative|base-search-card--link"))
                    if not link or "href" not in link.attrs: continue
                    
                    # Log debug link tìm thấy
                    raw_url = link["href"]
                    url = raw_url.split("?")[0]
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
            location = await self._safe_extract(soup, "location", rules, html)
            posted_date_raw = await self._safe_extract(soup, "posted_date", rules, html)
            industry = await self._safe_extract(soup, "industry", rules, html)
            job_function = await self._safe_extract(soup, "job_function", rules, html)

            # Fallback selectors if rules are not in DB yet
            if not location:
                loc_elem = soup.select_one(".topcard__flavor--bullet")
                location = loc_elem.get_text(strip=True) if loc_elem else "N/A"
            
            if not posted_date_raw:
                date_elem = soup.select_one(".posted-time-ago__text")
                posted_date_raw = date_elem.get_text(strip=True) if date_elem else None
            
            if not industry:
                # Tìm element chứa "Industries" và lấy span kế tiếp
                ind_elem = soup.find("h3", string=re.compile(r"Industries", re.I))
                if ind_elem:
                    industry = ind_elem.find_next("span").get_text(strip=True)
                else:
                    industry = "N/A"

            if not job_function:
                func_elem = soup.find("h3", string=re.compile(r"Job function", re.I))
                if func_elem:
                    job_function = func_elem.find_next("span").get_text(strip=True)
                else:
                    job_function = "N/A"

            # Parse date
            posted_date = self._parse_relative_date(posted_date_raw)

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
                "location": location,
                "posted_date": posted_date,
                "industry": industry,
                "job_function": job_function,
                "url": url,
                "site_name": "linkedin"
            }
        except Exception as e:
            logger.error("linkedin.extract.failed", url=url, error=str(e))
            return None

    def _parse_relative_date(self, date_text: Optional[str]) -> Optional[datetime.datetime]:
        if not date_text: return None
        now = datetime.datetime.now()
        match = re.search(r'(\d+)\s+(day|week|month|year|hour|minute)s?\s+ago', date_text.lower())
        if not match:
            if 'today' in date_text.lower() or 'just now' in date_text.lower():
                return now
            return None
        
        val = int(match.group(1))
        unit = match.group(2)
        
        if unit == 'day': return now - datetime.timedelta(days=val)
        if unit == 'week': return now - datetime.timedelta(weeks=val)
        if unit == 'month': return now - datetime.timedelta(days=val*30)
        if unit == 'year': return now - datetime.timedelta(days=val*365)
        if unit == 'hour': return now - datetime.timedelta(hours=val)
        if unit == 'minute': return now - datetime.timedelta(minutes=val)
        return None
