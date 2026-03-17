import asyncio
import sys
import json
import os
import csv
import datetime
import re
import aiohttp
import logging
import random
from typing import List, Set, Dict, Optional
from bs4 import BeautifulSoup
from urllib.parse import quote

# --- WINDOWS FIXES ---
if sys.platform == "win32":
    # Fix for UnicodeEncodeError in console (Vietnamese characters)
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    # Fix for "RuntimeError: Event loop is closed" on Windows
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# --- CONFIGURATION ---
# Base URLs
GUEST_SEARCH_URL = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
GUEST_DETAIL_URL = "https://www.linkedin.com/jobs-guest/jobs/api/jobPosting"
VOYAGER_API_URL = "https://www.linkedin.com/voyager/api/voyagerJobsDashJobCards"

# Files
SUMMARY_FILE = "linkedin_job_summaries.jsonl"
KEYWORDS_FILE = "keywords.txt"
CSV_OUTPUT = "linkedin_jobs_vietnam_it_full_v2.csv"

# Concurrency & Rates (Conservative to avoid 429)
MAX_CONCURRENT_TASKS = 3      # Number of concurrent job detail fetches
MAX_CONCURRENT_SEARCHES = 1   # Only search one keyword at a time to stay safe
RETRY_ATTEMPTS = 5
BASE_RETRY_DELAY = 10         # Seconds (will be exponential)
REQUEST_TIMEOUT = 30
SEARCH_PAGE_SIZE = 25         # LinkedIn guest API allows up to 25-50 usually

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
]

# Logging Setup
# Ensure logging handles UTF-8 correctly, especially on Windows
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')

# File Handler with UTF-8
file_handler = logging.FileHandler("scraper_v2.log", encoding='utf-8')
file_handler.setFormatter(formatter)

# Stream Handler with UTF-8 (sys.stdout might need wrapping or environment variable)
# But forcing UTF-8 on the stream handler is generally safer
import sys
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)

class LinkedInScraperV2:
    def __init__(self, cookies: Optional[Dict] = None):
        self.seen_job_ids = self._load_existing_ids()
        self.keywords = self._load_keywords()
        self.job_queue = []
        self.cookies = cookies
        self.session: Optional[aiohttp.ClientSession] = None
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
        self.search_semaphore = asyncio.Semaphore(MAX_CONCURRENT_SEARCHES)
        
    def _load_keywords(self) -> List[str]:
        if not os.path.exists(KEYWORDS_FILE):
            logger.warning(f"{KEYWORDS_FILE} not found. Using defaults.")
            return ["data engineer", "software engineer", "python developer"]
        with open(KEYWORDS_FILE, "r", encoding="utf-8") as f:
            return [line.strip() for line in f if line.strip()]

    def _load_existing_ids(self) -> Set[str]:
        ids = set()
        if os.path.exists(SUMMARY_FILE):
            with open(SUMMARY_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        data = json.loads(line)
                        if "job_id" in data:
                            ids.add(str(data["job_id"]))
                    except: pass
        logger.info(f"Loaded {len(ids)} existing job IDs from {SUMMARY_FILE}")
        return ids

    def get_headers(self):
        return {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
        }

    async def init_session(self):
        if self.session:
            await self.session.close()
        self.session = aiohttp.ClientSession(
            headers=self.get_headers(),
            cookies=self.cookies,
            timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        )

    async def fetch_search_results(self, keyword: str):
        """Fetch job list using Guest API with keyword search"""
        async with self.search_semaphore:
            logger.info(f"Searching for: {keyword.upper()}")
            start = 0
            empty_pages = 0
            
            while empty_pages < 2:  # Stop if 2 consecutive pages are empty
                params = {
                    "keywords": keyword,
                    "location": "Vietnam",
                    "start": start,
                    "count": SEARCH_PAGE_SIZE
                }
                
                try:
                    async with self.session.get(GUEST_SEARCH_URL, params=params) as resp:
                        if resp.status == 429:
                            wait = BASE_RETRY_DELAY * 2
                            logger.warning(f"429 Rate Limit on Search. Waiting {wait}s...")
                            await asyncio.sleep(wait)
                            continue
                        
                        if resp.status != 200:
                            logger.error(f"Search error {resp.status} for {keyword} at {start}")
                            break
                        
                        html = await resp.text()
                        soup = BeautifulSoup(html, "html.parser")
                        job_cards = soup.find_all("li")
                        
                        if not job_cards:
                            logger.info(f"End of results for '{keyword}' at start={start}")
                            empty_pages += 1
                            start += SEARCH_PAGE_SIZE
                            await asyncio.sleep(2)
                            continue
                        
                        empty_pages = 0
                        new_on_page = 0
                        
                        for card in job_cards:
                            link = card.find("a", class_=re.compile(r"base-card__full-link|result-card__full-link"))
                            if not link or "href" not in link.attrs: continue
                            
                            url = link["href"].split("?")[0]
                            # Extract Job ID
                            match = re.search(r"-(\d+)", url) or re.search(r"/view/(\d+)", url)
                            if not match: continue
                            
                            job_id = match.group(1)
                            if job_id in self.seen_job_ids: continue
                            
                            title_elem = card.find("h3", class_=re.compile(r"base-search-card__title|result-card__title"))
                            company_elem = card.find("a", class_=re.compile(r"hidden-nested-link|result-card__subtitle-link"))
                            
                            self.job_queue.append({
                                "job_id": job_id,
                                "url": url,
                                "title": title_elem.text.strip() if title_elem else "N/A",
                                "company": company_elem.text.strip() if company_elem else "N/A"
                            })
                            self.seen_job_ids.add(job_id)
                            new_on_page += 1
                        
                        logger.info(f"Keyword '{keyword}': Found {new_on_page} new jobs on page start={start}")
                        start += SEARCH_PAGE_SIZE
                        await asyncio.sleep(random.uniform(1.5, 3.0)) # Polite delay
                        
                except Exception as e:
                    logger.error(f"Exception during search for '{keyword}': {str(e)}")
                    break

    async def fetch_job_detail(self, job_item: Dict) -> Optional[Dict]:
        """Fetch details for a single job"""
        async with self.semaphore:
            job_id = job_item["job_id"]
            url = f"{GUEST_DETAIL_URL}/{job_id}"
            
            for attempt in range(RETRY_ATTEMPTS):
                try:
                    async with self.session.get(url) as resp:
                        if resp.status == 429:
                            wait = BASE_RETRY_DELAY * (2 ** attempt) + random.uniform(1, 5)
                            logger.warning(f"429 Limit for {job_id}. Retrying in {wait:.1f}s...")
                            await asyncio.sleep(wait)
                            continue
                            
                        if resp.status != 200:
                            logger.debug(f"Detail status {resp.status} for {job_id}")
                            return None
                        
                        html = await resp.text()
                        return self._parse_detail(html, job_item)
                        
                except Exception as e:
                    logger.error(f"Error fetching {job_id}: {str(e)}")
                    await asyncio.sleep(2)
            return None

    def _parse_detail(self, html: str, job_item: Dict) -> Dict:
        soup = BeautifulSoup(html, "html.parser")
        
        # Try to find JSON-LD first as it's the most reliable
        json_ld_script = soup.find("script", type="application/ld+json")
        json_data = {}
        if json_ld_script:
            try:
                json_data = json.loads(json_ld_script.string)
            except: pass
            
        # Fallback to HTML selectors
        description = "N/A"
        desc_div = soup.find("div", class_="show-more-less-html__markup")
        if desc_div:
            description = desc_div.get_text(separator="\n", strip=True)
        elif "description" in json_data:
            description = json_data["description"]
            
        location = "N/A"
        loc_span = soup.find("span", class_="topcard__flavor--bullet")
        if loc_span:
            location = loc_span.get_text(strip=True)
        elif "jobLocation" in json_data:
            location = json_data["jobLocation"].get("address", {}).get("addressLocality", "N/A")

        criteria = {}
        crit_items = soup.find_all("li", class_="description__job-criteria-item")
        for item in crit_items:
            key = item.find("h3").get_text(strip=True) if item.find("h3") else "Unknown"
            val = item.find("span").get_text(strip=True) if item.find("span") else "N/A"
            criteria[key] = val

        return {
            "job_id": job_item["job_id"],
            "job_title": job_item["title"],
            "company": job_item["company"],
            "location": location,
            "description": description[:3000],  # Limit length for CSV safety
            "posting_date": json_data.get("datePosted", "N/A"),
            "contract_type": criteria.get("Employment type", "N/A"),
            "job_function": criteria.get("Job function", "N/A"),
            "industry": criteria.get("Industries", "N/A"),
            "job_url": job_item["url"],
            "crawled_at": datetime.datetime.now().isoformat()
        }

    def _save_job(self, job: Dict, csv_writer, csv_file_handle):
        # Save to CSV
        csv_writer.writerow(job)
        csv_file_handle.flush()
        
        # Save to JSONL (Summary)
        summary = {
            "job_id": job["job_id"],
            "title": job["job_title"],
            "entityUrn": job["job_url"],
            "crawled_at": job["crawled_at"]
        }
        with open(SUMMARY_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(summary, ensure_ascii=False) + "\n")

    async def run(self):
        await self.init_session()
        
        # Step 1: Gathering Job IDs
        logger.info("--- PHASE 1: SEARCHING ---")
        for kw in self.keywords:
            await self.fetch_search_results(kw)
        
        total_to_scrape = len(self.job_queue)
        logger.info(f"Finished searching. {total_to_scrape} jobs in queue.")
        
        if total_to_scrape == 0:
            logger.info("No new jobs to scrape. Exiting.")
            await self.session.close()
            return

        # Step 2: Fetching Details
        logger.info("--- PHASE 2: SCRAPING DETAILS ---")
        
        file_exists = os.path.exists(CSV_OUTPUT)
        with open(CSV_OUTPUT, 'a', newline='', encoding='utf-8-sig') as csvfile:
            fieldnames = ['job_id', 'job_title', 'company', 'location', 'description', 
                         'posting_date', 'contract_type', 'job_function', 
                         'industry', 'job_url', 'crawled_at']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if not file_exists or os.stat(CSV_OUTPUT).st_size == 0:
                writer.writeheader()

            count = 0
            # Process in small batches to show progress
            while self.job_queue:
                batch = []
                # Grab a batch of jobs
                for _ in range(MAX_CONCURRENT_TASKS):
                    if not self.job_queue: break
                    job_item = self.job_queue.pop(0)
                    batch.append(self.fetch_job_detail(job_item))
                
                results = await asyncio.gather(*batch)
                
                for res in results:
                    if res:
                        self._save_job(res, writer, csvfile)
                        count += 1
                        if count % 10 == 0:
                            logger.info(f"Progress: {count}/{total_to_scrape} jobs saved.")
                
                await asyncio.sleep(0.5) # Small batch delay

        logger.info(f"Scraping complete. Total new jobs scraped: {count}")
        await self.session.close()

if __name__ == "__main__":
    # Note: For Airflow, you could pass specific keywords or ranges via sys.argv
    import sys
    
    scraper = LinkedInScraperV2()
    try:
        asyncio.run(scraper.run())
    except KeyboardInterrupt:
        logger.info("Stopped by user.")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
