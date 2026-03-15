import asyncio
import csv
import os
import re
from datetime import datetime
from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode, UndetectedAdapter
from crawl4ai.async_crawler_strategy import AsyncPlaywrightCrawlerStrategy

# === CẤU HÌNH CHỐNG DETECT (dựa docs Undetected Browser Mode) ===
MAX_RETRIES = 3
BATCH_SIZE = 10           # Giữ nguyên cũ
DELAY_BETWEEN_BATCHES = 5 # Giữ nguyên cũ
RETRY_DELAY = 10          # Giữ nguyên cũ

# BrowserConfig: Bật stealth + undetected
browser_config = BrowserConfig(
    headless=False,               # Visible mode để giảm detect Cloudflare (docs khuyến cáo)
    verbose=True,
    enable_stealth=True,          # Bật playwright-stealth layer
    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    viewport_width=1280,
    viewport_height=800,
    # proxy_config={  # Uncomment nếu có residential proxy VN
    #     "server": "http://user:pass@ip:port",
    #     "username": "user",
    #     "password": "pass"
    # },
)

# Undetected Adapter (deep patches chống fingerprinting nâng cao)
undetected_adapter = UndetectedAdapter()

# Custom strategy: Kết hợp undetected + stealth
crawler_strategy = AsyncPlaywrightCrawlerStrategy(
    browser_config=browser_config,
    browser_adapter=undetected_adapter
)

# Run config cho list page (giữ nguyên delay cũ)
list_run_config = CrawlerRunConfig(
    cache_mode=CacheMode.BYPASS,
    remove_overlay_elements=True,
    process_iframes=True,
    wait_for="css:div.job-list-search-result",
    page_timeout=100000,
    js_code="window.scrollTo(0, document.body.scrollHeight);",  # Giữ nguyên cũ
)

# Run config cho detail page (giữ nguyên delay cũ)
detail_run_config = CrawlerRunConfig(
    cache_mode=CacheMode.BYPASS,
    remove_overlay_elements=True,
    process_iframes=False,
    wait_for="css:.job-detail__body",
    page_timeout=30000,
)

base_url = "https://www.topcv.vn/tim-viec-lam-cong-nghe-thong-tin-cr257"

async def extract_number_pages():
    """Lấy tổng số trang từ pagination"""
    async with AsyncWebCrawler(
        crawler_strategy=crawler_strategy,
        config=browser_config
    ) as crawler:
        result = await crawler.arun(
            url=base_url,
            config=list_run_config
        )
        if not result.success:
            print(f"❌ Lỗi crawl trang đầu: {result.error_message}")
            return 1

        soup = BeautifulSoup(result.html, "html.parser")
        paginate_text = soup.select_one("#job-listing-paginate-text")
        if paginate_text:
            text = paginate_text.get_text(strip=True)
            try:
                max_page = int(text.split("/")[1].strip())
                print(f"✅ Tổng số trang: {max_page}")
                return max_page
            except:
                pass
        print("⚠️ Không tìm thấy pagination → giả sử 1 trang")
        return 1

def extract_url_jobs_from_result(result, all_job_links, data_file):
    """Parse list job từ HTML"""
    soup = BeautifulSoup(result.html, "html.parser")
    job_items = soup.select("div.job-item-search-result")
    new_jobs = 0
    for item in job_items:
        a_tag = item.select_one("a[href*='/viec-lam/']") or item.select_one("a[href]")
        href = a_tag["href"] if a_tag else None
        if href:
            clean_href = href.split("?")[0]
            full_url = "https://www.topcv.vn" + clean_href if clean_href.startswith("/") else clean_href
            if full_url not in all_job_links:
                all_job_links.add(full_url)
                with open(data_file, "a", encoding="utf-8") as f:
                    f.write(full_url + "\n")
                new_jobs += 1
            else:
                print(f"    🔁 Trùng: {clean_href}")
    return new_jobs, len(job_items)

async def crawl_topcv_url_jobs(max_pages=1):
    data_file = "data.csv"
    if os.path.exists(data_file):
        os.remove(data_file)
        print(f"Đã xóa {data_file} cũ.")

    all_job_links = set()
    page_urls = {i: f"{base_url}?page={i}" for i in range(1, max_pages + 1)}
    pending_pages = list(page_urls.keys())

    async with AsyncWebCrawler(
        crawler_strategy=crawler_strategy,
        config=browser_config
    ) as crawler:
        for attempt in range(1, MAX_RETRIES + 1):
            if not pending_pages:
                break
            if attempt > 1:
                print(f"\n{'='*50}\n🔄 RETRY {attempt}/{MAX_RETRIES} — {len(pending_pages)} pages còn lại\n{'='*50}")
                await asyncio.sleep(RETRY_DELAY)

            failed_this_round = []
            for batch_start in range(0, len(pending_pages), BATCH_SIZE):
                batch_pages = pending_pages[batch_start:batch_start + BATCH_SIZE]
                batch_urls = [page_urls[p] for p in batch_pages]
                print(f"\n[Batch {batch_start // BATCH_SIZE + 1}] Crawling pages {batch_pages} ...")

                results = await crawler.arun_many(
                    urls=batch_urls,
                    config=list_run_config
                )

                for page_num, result in zip(batch_pages, results):
                    if result.success:
                        new_jobs, total_items = extract_url_jobs_from_result(
                            result, all_job_links, data_file
                        )
                        print(f"  ✓ Page {page_num}: +{new_jobs}/{total_items} jobs (tổng: {len(all_job_links)})")
                    else:
                        failed_this_round.append(page_num)
                        print(f"  ✗ Page {page_num}: {result.error_message[:100]}...")

                if batch_start + BATCH_SIZE < len(pending_pages):
                    print(f"Chờ {DELAY_BETWEEN_BATCHES}s trước batch tiếp...")
                    await asyncio.sleep(DELAY_BETWEEN_BATCHES)

            pending_pages = failed_this_round

    print(f"\n{'='*50}")
    print(f"Total jobs found: {len(all_job_links)}")
    if pending_pages:
        print(f"Vẫn fail sau {MAX_RETRIES} retries: {pending_pages}")
    else:
        print("All list pages crawled OK!")
    print(f"{'='*50}")
    return all_job_links

# === PHẦN DETAIL ===
DETAIL_BATCH_SIZE = 20
DETAIL_DELAY = 2          # Giữ nguyên cũ
DETAIL_MAX_RETRIES = 3

CSV_HEADERS = [
    "job_id", "job_title", "company", "location", "description",
    "posting_date", "expiration_date", "job_type", "job_url", "crawled_date"
]

def extract_job_detail(html, url):
    soup = BeautifulSoup(html, "html.parser")
    m = re.search(r'/viec-lam/[^/]+/(\d+)', url)
    job_id = m.group(1) if m else ""

    job_title = (soup.select_one("h1.job-detail__info--title") or soup.select_one("h1[class*='title']") or {}).get_text(strip=True) or ""

    company = (soup.select_one("a.company-name-label") or soup.select_one(".company-name-label") or {}).get_text(strip=True) or ""

    location_parts = []
    for sec in soup.select(".job-detail__info--section"):
        label = sec.select_one(".job-detail__info--section-content-title")
        if label and "Địa điểm" in label.get_text():
            vals = sec.select(".job-detail__info--section-content-value")
            location_parts = [v.get_text(strip=True) for v in vals]
            break
    location = " | ".join(location_parts)

    desc_el = soup.select_one("div.job-detail__information-detail--content")
    description = desc_el.get_text(separator="\n", strip=True).replace("\r", "") if desc_el else ""

    expiration_date = ""
    deadline_el = soup.select_one(".job-detail__info--deadline")
    if deadline_el:
        text = deadline_el.get_text(strip=True)
        dm = re.search(r'(\d{2}/\d{2}/\d{4})', text)
        if dm:
            expiration_date = dm.group(1)

    job_type = ""
    for g in soup.select(".job-tags__group"):
        name = g.select_one(".job-tags__group-name")
        if name and "Chuyên môn" in name.get_text(strip=True):
            tag = g.select_one(".job-tags__group-list-tag-scroll a")
            job_type = tag.get_text(strip=True) if tag else ""
            break

    crawled_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return {
        "job_id": job_id,
        "job_title": job_title,
        "company": company,
        "location": location,
        "description": description,
        "expiration_date": expiration_date,
        "job_type": job_type,
        "job_url": url.split("?")[0],
        "crawled_date": crawled_date,
    }

async def crawl_job_details(url_file="data.csv", output_csv="jobs.csv"):
    if not os.path.exists(url_file):
        print(f"Không tìm thấy {url_file}. Chạy phase 1 trước!")
        return

    with open(url_file, "r", encoding="utf-8") as f:
        urls = [line.strip() for line in f if line.strip()]

    print(f"Tổng {len(urls)} job URLs.")

    crawled_urls = set()
    if os.path.exists(output_csv):
        with open(output_csv, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                crawled_urls.add(row.get("job_url", ""))
        print(f"Đã crawl {len(crawled_urls)} jobs trước đó. Resume...")

    pending_urls = [u for u in urls if u.split("?")[0] not in crawled_urls]
    print(f"Cần crawl: {len(pending_urls)} jobs mới.")

    if not pending_urls:
        print("Không có job mới.")
        return

    write_header = not os.path.exists(output_csv)
    with open(output_csv, "a", newline='', encoding="utf-8-sig") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=CSV_HEADERS)
        if write_header:
            writer.writeheader()

        failed_urls = list(pending_urls)
        total_success = len(crawled_urls)

        async with AsyncWebCrawler(
            crawler_strategy=crawler_strategy,
            config=browser_config
        ) as crawler:
            for attempt in range(1, DETAIL_MAX_RETRIES + 1):
                if not failed_urls:
                    break
                current_urls = list(failed_urls)
                failed_urls = []

                total_batches = (len(current_urls) + DETAIL_BATCH_SIZE - 1) // DETAIL_BATCH_SIZE
                if attempt > 1:
                    print(f"\n🔄 RETRY {attempt}/{DETAIL_MAX_RETRIES} — {len(current_urls)} URLs")
                    await asyncio.sleep(RETRY_DELAY)

                for batch_start in range(0, len(current_urls), DETAIL_BATCH_SIZE):
                    batch = current_urls[batch_start:batch_start + DETAIL_BATCH_SIZE]
                    print(f"\n[Detail Batch {batch_start // DETAIL_BATCH_SIZE + 1}/{total_batches}] Crawling {len(batch)} jobs...")

                    results = await crawler.arun_many(urls=batch, config=detail_run_config)

                    for url, result in zip(batch, results):
                        if result.success:
                            try:
                                job = extract_job_detail(result.html, url)
                                writer.writerow(job)
                                csvfile.flush()
                                total_success += 1
                                print(f"  ✓ {job['job_id']}: {job['job_title'][:50]}...")
                            except Exception as e:
                                failed_urls.append(url)
                                print(f"  ✗ Parse error {url}: {e}")
                        else:
                            failed_urls.append(url)
                            err = result.error_message[:100] if result.error_message else "Unknown"
                            print(f"  ✗ Fail {url.split('/')[-1]}: {err}")

                    if batch_start + DETAIL_BATCH_SIZE < len(current_urls):
                        await asyncio.sleep(DETAIL_DELAY)

    print(f"\n{'='*50}")
    print(f"Hoàn tất: {total_success} jobs → {output_csv}")
    if failed_urls:
        print(f"Vẫn fail sau retries: {len(failed_urls)} URLs")
    print(f"{'='*50}")

if __name__ == "__main__":
    # Phase 1: Crawl danh sách (uncomment khi cần chạy lại)
    # pages = asyncio.run(extract_number_pages())
    # asyncio.run(crawl_topcv_url_jobs(max_pages=pages))

    # Phase 2: Crawl chi tiết
    asyncio.run(crawl_job_details(url_file="data.csv", output_csv="jobs.csv"))