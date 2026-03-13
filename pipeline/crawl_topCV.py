import asyncio
import csv
import os
import re
from datetime import datetime
from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler
from crawl4ai.async_configs import BrowserConfig, CrawlerRunConfig, CacheMode

MAX_RETRIES = 3           # Số lần retry tối đa cho các page lỗi
BATCH_SIZE = 10           # Số page crawl song song mỗi batch
DELAY_BETWEEN_BATCHES = 5 # Giây chờ giữa mỗi batch
RETRY_DELAY = 10          # Giây chờ trước mỗi vòng retry

browser_config = BrowserConfig(
        headless=True,
        verbose=True,
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
    )

run_config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        remove_overlay_elements=True,
        process_iframes=True,
        wait_for="css:div.job-list-search-result",
        page_timeout=100000,
        js_code="window.scrollTo(0, document.body.scrollHeight);",
    )

base_url = "https://www.topcv.vn/tim-viec-lam-cong-nghe-thong-tin-cr257"
    
async def extract_number_pages():
    async with AsyncWebCrawler(config=browser_config) as crawler:
        result = await crawler.arun(
                url=base_url
            )
        """Extract page numbers from pagination, return max page number"""
        soup = BeautifulSoup(result.html, "html.parser")
        text = soup.select_one("#job-listing-paginate-text").get_text(strip=True)
        page_num = int(text.split(" ")[0].split("/")[1])
        
        return page_num

# Schema csv: job_id, job_title, company, location, description, posting_date, expiration_date, job_type, job_url, crawled_date
def extract_url_jobs_from_result(result, all_job_links, data_file):
    """Parse HTML bằng BeautifulSoup, trả về số job mới tìm được."""
    soup = BeautifulSoup(result.html, "html.parser")
    job_items = soup.select("div.job-item-search-result")
    new_jobs = 0

    for item in job_items:
        a_tag = item.select_one("a[href*='/viec-lam/']")
        if not a_tag:
            a_tag = item.select_one("a[href]")
        href = a_tag["href"] if a_tag else None

        if href:
            clean_href = href.split("?")[0]
            if clean_href not in all_job_links:
                all_job_links.add(clean_href)
                with open(data_file, "a", encoding="utf-8") as f:
                    f.write(href + "\n")
                new_jobs += 1
            else:
                print(f"    🔁 Trùng lặp: {clean_href}")
        else:
            print(f"    ⚠ Job item không có link: {item.get('data-job-id', 'unknown')}")

    return new_jobs, len(job_items)


async def crawl_topcv_url_jobs(max_pages=1):
    data_file = "data.csv"
    if os.path.exists(data_file):
        os.remove(data_file)
        print(f"Đã xóa {data_file} cũ.")

    all_job_links = set()

    # Tạo mapping: page_num -> url
    page_urls = {i: f"{base_url}?page={i}" for i in range(1, max_pages + 1)}
    pending_pages = list(page_urls.keys())  # Danh sách page cần crawl

    async with AsyncWebCrawler(config=browser_config) as crawler:
        for attempt in range(1, MAX_RETRIES + 1):
            if not pending_pages:
                break

            if attempt > 1:
                print(f"\n{'='*50}")
                print(f"🔄 RETRY lần {attempt}/{MAX_RETRIES} — {len(pending_pages)} pages: {pending_pages}")
                print(f"{'='*50}")
                await asyncio.sleep(RETRY_DELAY)

            failed_this_round = []

            for batch_start in range(0, len(pending_pages), BATCH_SIZE):
                batch_pages = pending_pages[batch_start:batch_start + BATCH_SIZE]
                batch_urls = [page_urls[p] for p in batch_pages]

                batch_label = f"[Batch {batch_start // BATCH_SIZE + 1}]"
                print(f"\n{batch_label} Crawling pages {batch_pages} ...")

                results = await crawler.arun_many(
                    urls=batch_urls,
                    config=run_config,
                )

                for page_num, result in zip(batch_pages, results):
                    if result.success:
                        new_jobs, total_items = extract_url_jobs_from_result(
                            result, all_job_links, data_file
                        )
                        print(f"  ✓ Page {page_num}: {new_jobs}/{total_items} jobs "
                              f"(total: {len(all_job_links)})")
                    else:
                        failed_this_round.append(page_num)
                        print(f"  ✗ Page {page_num}: {result.error_message[:80]}")

                remaining_batches = (len(pending_pages) - batch_start - BATCH_SIZE) > 0
                if remaining_batches:
                    print(f"{batch_label} Chờ {DELAY_BETWEEN_BATCHES}s trước batch tiếp ...")
                    await asyncio.sleep(DELAY_BETWEEN_BATCHES)

            pending_pages = failed_this_round

    print(f"\n{'='*50}")
    print(f"Total jobs found: {len(all_job_links)}")
    if pending_pages:
        print(f"Still failed after {MAX_RETRIES} retries ({len(pending_pages)}): {pending_pages}")
    else:
        print("All pages crawled successfully!")
    print(f"{'='*50}")

    return all_job_links


DETAIL_BATCH_SIZE = 20
DETAIL_DELAY = 2
DETAIL_MAX_RETRIES = 3

detail_run_config = CrawlerRunConfig(
    cache_mode=CacheMode.BYPASS,
    remove_overlay_elements=True,
    process_iframes=False,  # Detail page không cần iframe
    wait_for="css:.job-detail__body",
    page_timeout=30000,  # 30s đủ cho detail page
)

CSV_HEADERS = [
    "job_id", "job_title", "company", "location", "description",
    "posting_date", "expiration_date", "job_type", "job_url", "crawled_date"
]


def extract_job_detail(html, url):
    """Parse HTML trang chi tiết job, trả về dict theo schema."""
    soup = BeautifulSoup(html, "html.parser")

    # job_id từ URL
    m = re.search(r'/viec-lam/[^/]+/(\d+)', url)
    job_id = m.group(1) if m else ""

    # job_title
    el = soup.select_one("h1.job-detail__info--title") or soup.select_one("h1[class*='title']")
    job_title = el.get_text(strip=True) if el else ""

    # company
    el = soup.select_one("a.company-name-label") or soup.select_one(".company-name-label")
    company = el.get_text(strip=True) if el else ""

    # location — lấy từ info section
    location_parts = []
    info_sections = soup.select(".job-detail__info--section")
    for sec in info_sections:
        label = sec.select_one(".job-detail__info--section-content-title")
        if label and "Địa điểm" in label.get_text():
            vals = sec.select(".job-detail__info--section-content-value")
            location_parts = [v.get_text(strip=True) for v in vals]
            break
    location = " | ".join(location_parts)

    # description
    el = soup.select_one("div.job-detail__information-detail--content")
    description = el.get_text(separator="\n", strip=True) if el else ""
    # Escape newlines/quotes cho CSV
    description = description.replace("\r", "")

    # expiration_date — "Hạn nộp hồ sơ: DD/MM/YYYY"
    expiration_date = ""
    el = soup.select_one(".job-detail__info--deadline")
    if el:
        text = el.get_text(strip=True)
        dm = re.search(r'(\d{2}/\d{2}/\d{4})', text)
        if dm:
            expiration_date = dm.group(1)

    # job_type — loại hình công việc
    groups = soup.select(".job-tags__group")

    for g in groups:
        name = g.select_one(".job-tags__group-name").get_text(strip=True)
        
        if "Chuyên môn" in name:
            job_type = g.select_one(".job-tags__group-list-tag-scroll a").get_text(strip=True)
            
    crawled_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return {
        "job_id": job_id,
        "job_title": job_title,
        "company": company,
        "location": location,
        "description": description,
        # "posting_date": posting_date,
        "expiration_date": expiration_date,
        "job_type": job_type,
        "job_url": url.split("?")[0],
        "crawled_date": crawled_date,
    }


async def crawl_job_details(url_file="data.csv", output_csv="jobs.csv"):
    """Đọc danh sách URL từ file, crawl chi tiết từng job, lưu CSV."""
    # Đọc URLs
    with open(url_file, "r", encoding="utf-8") as f:
        urls = [line.strip() for line in f if line.strip()]

    print(f"Tổng {len(urls)} job URLs cần crawl chi tiết.")

    # Kiểm tra đã crawl trước đó chưa (resume)
    crawled_urls = set()
    if os.path.exists(output_csv):
        with open(output_csv, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                crawled_urls.add(row.get("job_url", ""))
        print(f"Đã crawl trước đó: {len(crawled_urls)} jobs. Resume phần còn lại.")

    # Lọc bỏ URL đã crawl
    pending_urls = [u for u in urls if u.split("?")[0] not in crawled_urls]
    print(f"Cần crawl: {len(pending_urls)} jobs.")

    if not pending_urls:
        print("Không có job mới cần crawl.")
        return

    # Tạo CSV header nếu file mới
    write_header = not os.path.exists(output_csv)
    csv_file = open(output_csv, "a", encoding="utf-8-sig", newline="")
    writer = csv.DictWriter(csv_file, fieldnames=CSV_HEADERS)
    if write_header:
        writer.writeheader()

    failed_urls = list(pending_urls)
    total_success = len(crawled_urls)

    async with AsyncWebCrawler(config=browser_config) as crawler:
        for attempt in range(1, DETAIL_MAX_RETRIES + 1):
            if not failed_urls:
                break

            current_urls = list(failed_urls)
            failed_urls = []
            total_batches = (len(current_urls) + DETAIL_BATCH_SIZE - 1) // DETAIL_BATCH_SIZE

            if attempt > 1:
                print(f"\n🔄 RETRY lần {attempt}/{DETAIL_MAX_RETRIES} — {len(current_urls)} URLs")
                await asyncio.sleep(RETRY_DELAY)

            for batch_start in range(0, len(current_urls), DETAIL_BATCH_SIZE):
                batch = current_urls[batch_start:batch_start + DETAIL_BATCH_SIZE]
                batch_num = batch_start // DETAIL_BATCH_SIZE + 1
                print(f"\n[Detail Batch {batch_num}/{total_batches}] Crawling {len(batch)} jobs...")

                results = await crawler.arun_many(urls=batch, config=detail_run_config)

                for url, result in zip(batch, results):
                    if result.success:
                        try:
                            job = extract_job_detail(result.html, url)
                            writer.writerow(job)
                            csv_file.flush()
                            total_success += 1
                            print(f"  ✓ {job['job_id']}: {job['job_title'][:50]}")
                        except Exception as e:
                            failed_urls.append(url)
                            print(f"  ✗ Parse error {url}: {e}")
                    else:
                        failed_urls.append(url)
                        err = result.error_message[:80] if result.error_message else "Unknown"
                        print(f"  ✗ Crawl failed {url.split('/')[-1]}: {err}")

                # Delay giữa batch
                if batch_start + DETAIL_BATCH_SIZE < len(current_urls):
                    await asyncio.sleep(DETAIL_DELAY)

    csv_file.close()

    print(f"\n{'='*50}")
    print(f"Crawl chi tiết hoàn tất: {total_success} jobs → {output_csv}")
    if failed_urls:
        print(f"Vẫn lỗi sau {DETAIL_MAX_RETRIES} lần: {len(failed_urls)} URLs")
    print(f"{'='*50}")

async def crawl_job_details_test():
    url = "https://www.topcv.vn/viec-lam/quan-tri-vien-tap-su-quan-ly-du-an-future-changemakers/2040774.html?ta_source=JobSearchList_LinkDetail&u_sr_id=IUgf1gTnSRD5xpLiuJjqgLTnBIIXi20NCXLHFCYY_1772768575"
    async with AsyncWebCrawler(config=browser_config) as crawler:
        result = await crawler.arun(
                url=url
            )
        """Extract page numbers from pagination, return max page number"""
        soup = BeautifulSoup(result.html, "html.parser")

        # job_id từ URL
        m = re.search(r'/viec-lam/[^/]+/(\d+)', url)
        job_id = m.group(1) if m else ""

        # job_title
        el = soup.select_one("h1.job-detail__info--title") or soup.select_one("h1[class*='title']")
        job_title = el.get_text(strip=True) if el else ""

        # company
        el = soup.select_one("a.company-name-label") or soup.select_one(".company-name-label")
        company = el.get_text(strip=True) if el else ""

        # location — lấy từ info section
        location_parts = []
        info_sections = soup.select(".job-detail__info--section")
        for sec in info_sections:
            label = sec.select_one(".job-detail__info--section-content-title")
            if label and "Địa điểm" in label.get_text():
                vals = sec.select(".job-detail__info--section-content-value")
                location_parts = [v.get_text(strip=True) for v in vals]
                break
        location = " | ".join(location_parts)

        # description
        el = soup.select_one("div.job-detail__information-detail--content")
        description = el.get_text(separator="\n", strip=True) if el else ""
        # Escape newlines/quotes cho CSV
        description = description.replace("\r", "")

        # expiration_date — "Hạn nộp hồ sơ: DD/MM/YYYY"
        expiration_date = ""
        el = soup.select_one(".job-detail__info--deadline")
        if el:
            text = el.get_text(strip=True)
            dm = re.search(r'(\d{2}/\d{2}/\d{4})', text)
            if dm:
                expiration_date = dm.group(1)

        # job_type — loại hình công việc
        groups = soup.select(".job-tags__group")

        for g in groups:
            name = g.select_one(".job-tags__group-name").get_text(strip=True)
            
            if "Chuyên môn" in name:
                job_type = g.select_one(".job-tags__group-list-tag-scroll a").get_text(strip=True)
                
        crawled_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        return {
            "job_id": job_id,
            "job_title": job_title,
            "company": company,
            "location": location,
            "description": description,
            # "posting_date": posting_date,
            "expiration_date": expiration_date,
            "job_type": job_type,
            "job_url": url.split("?")[0],
            "crawled_date": crawled_date,
        }

if __name__ == "__main__":
    # # Phase 1: Crawl danh sách URL jobs
    # pages = asyncio.run(extract_number_pages())
    # asyncio.run(crawl_topcv_url_jobs(max_pages=pages))

    # Phase 2: Crawl chi tiết từng job → lưu CSV
    asyncio.run(crawl_job_details(url_file="data.csv", output_csv="jobs.csv"))

    # Test crawl chi tiết 1 job
    # job_detail = asyncio.run(crawl_job_details_test())
    # import pandas as pd
    # df = pd.DataFrame([job_detail])
    # print(df)
    # df.to_csv("test_job_detail.csv", index=False, encoding="utf-8-sig")
