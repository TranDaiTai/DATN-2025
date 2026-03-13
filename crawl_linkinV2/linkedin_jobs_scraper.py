import asyncio
import json
import os
import csv
import datetime
import re
import time
import requests
from typing import List, Set, Dict, Optional
from bs4 import BeautifulSoup

# --- CẤU HÌNH HỆ THỐNG ---
API_SEARCH_URL = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
API_DETAIL_URL = "https://www.linkedin.com/jobs-guest/jobs/api/jobPosting"
SUMMARY_FILE = "linkedin_job_summaries.jsonl"
KEYWORDS_FILE = "keywords.txt"
CSV_OUTPUT = "linkedin_jobs_vietnam_it_full.csv"
BATCH_SIZE = 10
REQUEST_TIMEOUT = (30, 90)  # Tăng timeout: (30s connect, 90s read)
RETRY_ATTEMPTS = 4
RETRY_DELAY = 2
MAX_CONCURRENT = 5  # Concurrent detail scrapes
REQUEST_DELAY = 0.2  # Delay between request batches
MAX_CONCURRENT_SEARCH = 4  # Concurrent search keywords
SEARCH_DELAY = 0.2  # Delay between search pages (mới - tăng từ 1s)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
}

class LinkedInCrawl:
    def __init__(self):
        self.seen_job_ids = self._load_existing_ids()
        self.keywords = self._load_keywords()
        self.queue = []
        self.session = requests.Session()
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT)
        self.lock = asyncio.Lock()
    
    def _sanitize_filename(self, filename: str) -> str:
        """Loại bỏ ký tự không hợp lệ trong tên file"""
        invalid_chars = r'[/\\:*?"<>|]'
        return re.sub(invalid_chars, '_', filename)

    def _load_keywords(self) -> List[str]:
        if not os.path.exists(KEYWORDS_FILE):
            defaults = ["data engineer"]
            with open(KEYWORDS_FILE, "w", encoding="utf-8") as f:
                f.write("\n".join(defaults))
            return defaults
        with open(KEYWORDS_FILE, "r", encoding="utf-8") as f:
            return [line.strip() for line in f if line.strip()]

    def _load_existing_ids(self) -> Set[str]:
        ids = set()
        if os.path.exists(SUMMARY_FILE):
            with open(SUMMARY_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        ids.add(json.loads(line).get("job_id"))
                    except: pass
        print(f"[*] Đã tải {len(ids)} job IDs từ bộ nhớ cũ.")
        return ids

    async def get_search_results(self, keyword: str):
        """Gọi LinkedIn API để lấy danh sách job theo keyword (tối đa ~1000 jobs)"""
        print(f"\n🔎 Đang tìm kiếm: {keyword.upper()}")
        
        all_found = 0
        page_start = 0
        
        while True:
            params = {
                "keywords": keyword,
                "location": "Vietnam",
                "start": page_start,
                "count": 10
            }
            
            try:
                # No global semaphore - let 429 backoff handle rate limiting
                response = self.session.get(API_SEARCH_URL, params=params, headers=HEADERS, timeout=REQUEST_TIMEOUT)
                response.encoding = 'utf-8'
                
                # Đợi thêm xíu để response hoàn toàn được nhận
                await asyncio.sleep(0.2)
                
                # Debug: In response headers và status
                print(f"   Status: {response.status_code}, Content-Type: {response.headers.get('content-type', 'unknown')}")
                
                # Parse HTML
                soup = BeautifulSoup(response.text, "html.parser")
                
                # Try multiple selectors to find job items
                job_items = soup.find_all("li", class_="base-search-card__result-item")
                if not job_items:
                    job_items = soup.find_all("li", class_="jobs-search__results-list__result-item")
                if not job_items:
                    job_items = soup.find_all("div", class_="base-card")
                if not job_items:
                    job_items = soup.find_all("li")
                print(f"   [DEBUG] Found {len(job_items)} items with selectors")
                
                if not job_items:
                    print(f"⚠️ Không tìm thấy job - dừng trang {page_start // 10 + 1}")
                    break
                    
                new_count = 0
                for job_item in job_items:
                    try:
                        link = job_item.find("a", class_="base-card__full-link")
                        if not link or "href" not in link.attrs:
                            continue
                        
                        url = link["href"].replace("vn.linkedin.com", "www.linkedin.com")
                        
                        match = re.search(r'-(\d+)\?', url) or re.search(r'/view/(\d+)', url)
                        if not match:
                            continue
                        
                        job_id = match.group(1)
                        if job_id in self.seen_job_ids:
                            continue
                        
                        title_elem = job_item.find("h3", class_="base-search-card__title")
                        title = title_elem.text.strip() if title_elem else "N/A"
                        
                        company_elem = job_item.find("a", class_="hidden-nested-link")
                        company = company_elem.text.strip() if company_elem else "N/A"
                        
                        time_elem = job_item.find("time")
                        posting_date = time_elem.get("datetime", "N/A") if time_elem else "N/A"
                        
                        self.queue.append({
                            'job_id': job_id,
                            'url': url,
                            'title': title,
                            'company': company,
                            'posting_date': posting_date
                        })
                        self.seen_job_ids.add(job_id)
                        new_count += 1
                    except Exception as e:
                        pass
                
                print(f"✅ Trang {page_start // 10 + 1}: +{new_count} job")
                all_found += new_count
                
                page_start += 10
                await asyncio.sleep(SEARCH_DELAY)  # Delay giữa các trang (tăng để đợi phản hồi)
                
            except Exception as e:
                error_msg = str(e)
                if 'timeout' in error_msg.lower() or 'timed out' in error_msg.lower():
                    print(f"⏱️ Timeout - server chậm, chờ 5s rồi thử lại...")
                    await asyncio.sleep(5)
                    continue
                else:
                    print(f"❌ Lỗi search: {error_msg}")
                    break
        
        print(f"✅ Tổng: +{all_found} job từ '{keyword}'")
            
    async def scrap_detail(self, job_item: Dict) -> Optional[Dict]:
        """Gọi API LinkedIn để lấy chi tiết job posting + kiểm soát concurrency"""
        
        async with self.semaphore:  # Giới hạn số request đồng thời
            api_url = f"{API_DETAIL_URL}/{job_item['job_id']}"
            
            for attempt in range(RETRY_ATTEMPTS):
                try:
                    response = self.session.get(api_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
                    
                    if response.status_code == 429:
                        # Rate limit - exponential backoff
                        if attempt < RETRY_ATTEMPTS - 1:
                            wait_time = RETRY_DELAY * (2 ** attempt)  # Exponential: 2s, 4s, 8s, 16s
                            print(f"   ⏳ 429 - chờ {wait_time}s...")
                            await asyncio.sleep(wait_time)
                            continue
                        return None  # Give up after max attempts
                    elif response.status_code != 200:
                        if attempt < RETRY_ATTEMPTS - 1:
                            await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                            continue
                        print(f"   ⚠️ Status {response.status_code} cho {job_item['job_id']}")
                        return None
                    
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    # Extract data
                    job_title = job_item['title']
                    company = job_item.get('company', 'N/A')
                    posting_date = job_item.get('posting_date', 'N/A')
                    
                    # Location
                    location = "N/A"
                    for span in soup.find_all('span'):
                        text = span.get_text(strip=True)
                        if any(loc in text for loc in ['City', 'Vietnam', 'District']) and not any(time in text for time in ['days ago', 'hours ago']):
                            location = text
                            break
                    
                    # Description
                    description = "N/A"
                    desc_div = soup.find('div', class_='show-more-less-html__markup')
                    if desc_div:
                        for btn in desc_div.find_all('button'):
                            btn.decompose()
                        description = desc_div.get_text(separator='\n', strip=True)[:1500]
                    
                    # Criteria
                    job_function = contract_type = industry = "N/A"
                    criteria_list = soup.find('ul', class_='description__job-criteria-list')
                    if criteria_list:
                        criteria_items = criteria_list.find_all('li', class_='description__job-criteria-item')
                        
                        if len(criteria_items) > 1:
                            span = criteria_items[1].find('span', class_='description__job-criteria-text')
                            contract_type = span.get_text(strip=True) if span else "N/A"
                        
                        if len(criteria_items) > 2:
                            span = criteria_items[2].find('span', class_='description__job-criteria-text')
                            job_function = span.get_text(strip=True) if span else "N/A"
                        
                        if len(criteria_items) > 3:
                            span = criteria_items[3].find('span', class_='description__job-criteria-text')
                            industry = span.get_text(strip=True) if span else "N/A"
                    
                    return {
                        'job_id': job_item['job_id'],
                        'job_title': job_title.strip(),
                        'company': company.strip(),
                        'location': location.strip(),
                        'description': description,
                        'posting_date': posting_date,
                        'contract_type': contract_type,
                        'job_function': job_function,
                        'industry': industry,
                        'job_url': job_item['url']
                    }
                    
                except Exception as e:
                    if attempt < RETRY_ATTEMPTS - 1:
                        error_msg = str(e)
                        if 'timeout' in error_msg.lower():
                            print(f"   ⏱️ Timeout - chờ 3s...")
                            await asyncio.sleep(3)
                        else:
                            await asyncio.sleep(REQUEST_DELAY * (attempt + 1))
                    else:
                        print(f"   ⚠️ Exception {str(e)[:40]} cho {job_item['job_id']}")
                        return None
            
            await asyncio.sleep(REQUEST_DELAY)
    
    def _init_csv_writer(self):
        """Khởi tạo CSV writer (gọi 1 lần khi bắt đầu)"""
        file_exists = os.path.exists(CSV_OUTPUT)
        csvfile = open(CSV_OUTPUT, 'a', newline='', encoding='utf-8-sig')
        fieldnames = ['job_id', 'job_title', 'company', 'location', 'description', 
                     'posting_date', 'contract_type', 'job_function', 
                     'industry', 'job_url', 'crawled_at']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        # Ghi header nếu file mới
        if not file_exists or os.stat(CSV_OUTPUT).st_size == 0:
            writer.writeheader()
        
        return csvfile, writer
    
    def _save_job_success(self, csvfile, csv_writer, job_item: Dict, job_detail: Dict):
        """Lưu job thành công - ghi vào CSV + JSONL (like notebook)"""
        # --- Ghi CSV (đầy đủ các trường) ---
        csv_row = {
            'job_id': job_item['job_id'],
            'job_title': job_item['title'],
            'company': job_detail.get('company', 'N/A').strip(),
            'location': job_detail.get('location', 'N/A'),
            'description': job_detail.get('description', 'N/A'),
            'posting_date': job_detail.get('posting_date', 'N/A'),
            'contract_type': job_detail.get('contract_type', 'N/A'),
            'job_function': job_detail.get('job_function', 'N/A'),
            'industry': job_detail.get('industry', 'N/A'),
            'job_url': job_item['url'],
            'crawled_at': datetime.datetime.now().isoformat()
        }
        csv_writer.writerow(csv_row)
        csvfile.flush()
        
        # --- Ghi JSONL (không có company) ---
        jsonl_entry = {
            'job_id': job_item['job_id'],
            'title': job_item['title'],
            'entityUrn': job_item['url'],
            'crawled_at': datetime.datetime.now().isoformat()
        }
        with open(SUMMARY_FILE, 'a', encoding='utf-8') as f:
            f.write(json.dumps(jsonl_entry, ensure_ascii=False) + '\n')
    
    async def _process_job_concurrent(self, job_item: Dict, position: int, total: int, csvfile, csv_writer):
        """Xử lý một job trong chế độ song song"""
        try:
            title = job_item['title'][:40]
            print(f"\n[{position}/{total}] {title}...")
            
            item = await self.scrap_detail(job_item)
            if item:
                async with self.lock:
                    self._save_job_success(csvfile, csv_writer, job_item, item)
                print(f"✅ ĐÃ LẤY: {item['job_title'][:30]} | {item['company'][:20]}")
                return True
            else:
                print(f"❌ Lỗi detail {job_item['job_id']}")
                return False
        except Exception as e:
            print(f"❌ Lỗi xử lý: {str(e)[:50]}")
            return False
    
    async def run(self, max_jobs: int = None):
        """Chạy crawler với xử lý song song"""
        csvfile, csv_writer = self._init_csv_writer()
        count = failed_count = 0
        
        try:
            # Bước 1: Search (SONG SONG với giới hạn + staggered start)
            print("=" * 60)
            print("📋 BƯỚC 1: Quét danh sách job từ Keywords (SONG SONG)")
            print("=" * 60)
            
            # Chạy các search song song nhưng giới hạn số lượng đồng thời và stagger start
            async def search_with_stagger(kw, index):
                await asyncio.sleep(index * 2)  # Stagger start: 0s, 2s, 4s, 6s...
                await self.get_search_results(kw)
            
            search_tasks = [search_with_stagger(kw, idx) for idx, kw in enumerate(self.keywords)]
            await asyncio.gather(*search_tasks, return_exceptions=True)
            
            # Bước 2: Scrap details (song song)
            print("\n" + "=" * 60)
            print(f"🚀 BƯỚC 2: Lấy chi tiết {len(self.queue)} job (song song)")
            print("=" * 60)
            
            total = len(self.queue)
            position = 0
            
            # Xử lý jobs theo batch để hiển thị vị trí chính xác
            while self.queue:
                if max_jobs and count >= max_jobs:
                    break
                
                # Lấy batch của MAX_CONCURRENT jobs
                batch = []
                for _ in range(MAX_CONCURRENT):
                    if not self.queue:
                        break
                    position += 1
                    job = self.queue.pop(0)
                    batch.append(self._process_job_concurrent(job, position, total, csvfile, csv_writer))
                
                # Chạy batch song song
                results = await asyncio.gather(*batch, return_exceptions=True)
                for result in results:
                    if isinstance(result, bool) and result:
                        count += 1
                    elif isinstance(result, bool):
                        failed_count += 1
                
                await asyncio.sleep(0.1)  # Nhỏ delay giữa batch
            
        finally:
            csvfile.close()
            
            # Print final summary
            print("\n" + "=" * 60)
            print("💾 BƯỚC 3: Kết quả cuối cùng")
            print("=" * 60)
            print(f"✅ Hoàn thành!")
            print(f"   - Scraped: {count}")
            print(f"   - Failed: {failed_count}")
            print(f"   - Total IDs: {len(self.seen_job_ids)}")
            print(f"   - CSV: {CSV_OUTPUT}")
            print(f"   - JSONL: {SUMMARY_FILE}")
            print("=" * 60)

if __name__ == "__main__":
    scraper = LinkedInCrawl()
    try:
        # Có thể điều chỉnh max_jobs để giới hạn số lượng (None = không giới hạn)
        asyncio.run(scraper.run(max_jobs=None))
    except KeyboardInterrupt:
        print("\n[!] Đã dừng chương trình bởi người dùng.")
    finally:
        scraper.session.close()