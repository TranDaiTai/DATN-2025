import requests
import json
from urllib.parse import quote
import csv
import time
import datetime
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# ==================== CẤU HÌNH ====================
SUMMARY_FILE = "linkedin_job_summaries.jsonl"       # Lưu job đã xử lý THÀNH CÔNG (detail + CSV)
CSV_FILE = "linkedin_jobs_vietnam_it_full.csv"     # Lưu chi tiết job
COUNT = 100                                        # Số job mỗi trang
MAX_WORKERS = 8                                    # Luồng song song lấy detail
RETRY_COUNT = 3                                    # Retry khi lấy detail fail

base_url = "https://www.linkedin.com/voyager/api/voyagerJobsDashJobCards"

# Load keywords
def load_keywords(filename="keywords.txt"):
    try:
        with open(filename, "r", encoding="utf-8") as f:
            keywords = [line.strip() for line in f if line.strip()]
        print(f"Đã tải {len(keywords)} từ khóa: {keywords}\n")
        return keywords
    except FileNotFoundError:
        print("Không tìm thấy 'keywords.txt' → dùng mặc định: 'it'")
        return ["it"]

keywords_list = load_keywords()

# Headers & cookies (cập nhật thường xuyên từ DevTools)

headers = {
    "accept": os.getenv("LI_ACCEPT"),
    "csrf-token": os.getenv("LI_CSRF_TOKEN"),
    "referer": os.getenv("LI_REFERER"),
    "user-agent": os.getenv("LI_USER_AGENT"),
    "x-li-lang": os.getenv("LI_LANG"),
    "x-restli-protocol-version": os.getenv("LI_RESTLI_VERSION"),
}

cookies = {
    "li_at": os.getenv("LI_AT"),
    "JSESSIONID": f'"{os.getenv("JSESSIONID")}"',  # LinkedIn BẮT BUỘC có dấu "
}


session = requests.Session()

# ==================== LOAD EXISTING SUMMARIES (đã xử lý thành công) ====================
def load_existing_summaries():
    if not os.path.exists(SUMMARY_FILE):
        return set()
    existing_ids = set()
    with open(SUMMARY_FILE, "r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            if (i + 1) % 500 == 0:
                print(f"   Loading summaries cũ: {i+1} dòng...")
            if line.strip():
                try:
                    entry = json.loads(line)
                    existing_ids.add(entry['job_id'])
                except:
                    pass
    print(f"Đã load {len(existing_ids)} job đã xử lý thành công trước đó.\n")
    return existing_ids

# ==================== FETCH JOB LIST (chỉ lấy job mới & chưa trùng) ====================
def fetch_job_list(keyword, seen_job_ids):
    query_string_base = (
        f"decorationId=com.linkedin.voyager.dash.deco.jobs.search.JobSearchCardsCollectionLite-88"
        f"&count={COUNT}"
        f"&q=jobSearch"
        f"&query=(origin:SWITCH_SEARCH_VERTICAL,keywords:{keyword},spellCorrectionEnabled:true)"
        f"&servedEventEnabled=false"
    )

    new_jobs = []
    start = 0
    total = None
    page = 1

    print(f"[{keyword.upper()}] Bắt đầu tìm kiếm...")

    while total is None or start < total:
        print(f"   Trang {page} (start={start})...", end=" ")
        url = f"{base_url}?{query_string_base}&start={start}"
        try:
            response = session.get(url, headers=headers, cookies=cookies, timeout=30)
            if response.status_code != 200:
                print(f"HTTP {response.status_code}")
                break
            data = response.json()
        except Exception as e:
            print(f"Lỗi request: {e}")
            break

        included = data.get('included', [])
        if total is None:
            total = data.get('data', {}).get('paging', {}).get('total', 0)
            print(f"→ Tổng: {total}")

        jobs_found = 0
        for item in included:
            if (item.get('$type') == 'com.linkedin.voyager.dash.jobs.JobPosting' and
                item.get('entityUrn', '').startswith('urn:li:fsd_jobPosting:')):
                job_id = item['entityUrn'].split(':')[-1]
                if job_id not in seen_job_ids:
                    seen_job_ids.add(job_id)
                    new_jobs.append(item)
                    jobs_found += 1

        print(f"+{jobs_found} job duy nhất")
        if jobs_found == 0 and start > 0:
            break

        start += COUNT
        page += 1
        time.sleep(1)

    print(f"[{keyword.upper()}] Hoàn thành: +{len(new_jobs)} job mới\n")
    return new_jobs

# ==================== FETCH DETAIL (có retry) ====================
def fetch_job_detail(job_id):
    url_temp = "https://www.linkedin.com/voyager/api/graphql"
    urn_raw = f"urn:li:fsd_jobPosting:{job_id}"
    urn_encoded = quote(urn_raw, safe="")
    variables = f"(jobPostingUrn:{urn_encoded})"
    query_id = "voyagerJobsDashJobPostings.891aed7916d7453a37e4bbf5f1f60de4"
    url = f"{url_temp}?variables={variables}&queryId={query_id}"

    for attempt in range(RETRY_COUNT):
        try:
            response = session.get(url, headers=headers, cookies=cookies, timeout=20)
            if response.status_code != 200:
                if attempt == RETRY_COUNT - 1:
                    return None, None
                time.sleep(1)
                continue
            data = response.json()
            included = data.get('included', [])
            for item in included:
                if item.get('$type') == 'com.linkedin.voyager.dash.jobs.JobPosting':
                    return item, included
            return None, None
        except Exception:
            if attempt == RETRY_COUNT - 1:
                return None, None
            time.sleep(2 ** attempt)  # Exponential backoff
    return None, None

# ==================== EXTRACT INFO ====================
def extract_job_info(job_detail, included=None):
    if not job_detail:
        return None

    def resolve_urn(urn, target_type):
        if not urn or not included:
            return None
        for item in included:
            if item.get('entityUrn') == urn and item.get('$type') == target_type:
                return item
        return None

    job_id = job_detail.get('entityUrn', '').split(':')[-1]
    job_title = job_detail.get('title', '')
    company_name = job_detail.get('companyDetails', {}).get('name')

    location = None
    loc_urn = job_detail.get('*location')
    if loc_urn and isinstance(loc_urn, str):
        geo = resolve_urn(loc_urn, 'com.linkedin.voyager.dash.common.Geo')
        if geo:
            location = geo.get('defaultLocalizedName') or geo.get('abbreviatedLocalizedName')
    if not location:
        location = job_detail.get('formattedLocation')

    description = job_detail.get('description', {}).get('text', '')

    posting_date = None
    if job_detail.get('listedAt'):
        try:
            posting_date = datetime.datetime.fromtimestamp(job_detail['listedAt'] / 1000).strftime('%Y-%m-%d')
        except:
            pass

    expiration_date = None
    if job_detail.get('expireAt'):
        try:
            expiration_date = datetime.datetime.fromtimestamp(job_detail['expireAt'] / 1000).strftime('%Y-%m-%d')
        except:
            pass

    employment_type = None
    emp_urn = job_detail.get('*employmentStatus')
    if emp_urn and isinstance(emp_urn, str):
        emp = resolve_urn(emp_urn, 'com.linkedin.voyager.dash.hiring.EmploymentStatus')
        if emp:
            employment_type = emp.get('localizedName')

    job_functions = ', '.join(job_detail.get('jobFunctions', []))
    job_state = job_detail.get('jobState')

    industry = None
    ind_list = job_detail.get('*industryV2Taxonomy', [])
    if ind_list and included:
        names = []
        for u in ind_list:
            ind = resolve_urn(u, 'com.linkedin.voyager.dash.identity.profile.IndustryV2')
            if ind:
                names.append(ind.get('name'))
        industry = ', '.join(names) if names else None

    job_url = f"https://www.linkedin.com/jobs/view/{job_id}"

    return {
        'job_id': job_id,
        'job_title': job_title,
        'company': company_name,
        'location': location,
        'description': description,
        'posting_date': posting_date,
        'expiration_date': expiration_date,
        'contract_type': employment_type,
        'job_function': job_functions,
        'industry': industry,
        'job_state': job_state,
        'job_url': job_url,
        'crawled_at': datetime.datetime.now().isoformat(),
    }

# ==================== MAIN ====================
if __name__ == "__main__":
    # Load job đã xử lý thành công trước đó
    processed_job_ids = load_existing_summaries()

    # Set để deduplicate trong lần chạy này (bao gồm cả cũ)
    seen_job_ids = processed_job_ids.copy()

    all_new_jobs = []

    print(f"Đang tìm kiếm với {len(keywords_list)} từ khóa...\n")
    for idx, kw in enumerate(keywords_list, 1):
        print(f"[{idx}/{len(keywords_list)}] Từ khóa: '{kw}'")
        new_jobs = fetch_job_list(kw, seen_job_ids)
        all_new_jobs.extend(new_jobs)

    if not all_new_jobs:
        print("Không có job mới nào cần xử lý. Kết thúc.")
        exit()

    print(f"\nTổng cộng {len(all_new_jobs)} job mới & duy nhất cần lấy chi tiết.\n")

    file_exists = os.path.exists(CSV_FILE)
    with open(CSV_FILE, 'a', newline='', encoding='utf-8-sig', buffering=1) as csvfile:
        fieldnames = [
            'job_id', 'job_title', 'company', 'location', 'description',
            'posting_date', 'expiration_date', 'contract_type', 'job_function',
            'industry', 'job_state', 'job_url', 'crawled_at'
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if not file_exists or os.stat(CSV_FILE).st_size == 0:
            writer.writeheader()

        success = 0
        failed = 0

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(fetch_job_detail, job['entityUrn'].split(':')[-1]): job for job in all_new_jobs}

            processed = 0
            total = len(futures)
            for future in as_completed(futures):
                processed += 1
                job = futures[future]
                job_id = job['entityUrn'].split(':')[-1]
                title = job.get('title', 'No title')

                job_detail, included = future.result()
                source = job_detail or job
                info = extract_job_info(source, included=included)

                if info:
                    writer.writerow(info)
                    csvfile.flush()  # Thấy dữ liệu real-time

                    # CHỈ LÚC NÀY MỚI LƯU SUMMARY (an toàn)
                    entry = {
                        "job_id": job_id,
                        "title": job.get('title'),
                        "entityUrn": job['entityUrn'],
                        "crawled_at": datetime.datetime.now().isoformat(),
                        "raw_data": job  # hoặc job_detail nếu muốn lưu full
                    }
                    with open(SUMMARY_FILE, "a", encoding="utf-8") as f:
                        f.write(json.dumps(entry, ensure_ascii=False) + "\n")

                    success += 1
                    print(f"[{processed}/{total}] [OK] {job_id} - {title}")
                else:
                    failed += 1
                    print(f"[{processed}/{total}] [FAIL] {job_id} - {title}")

                time.sleep(0.25)  # Nhẹ nhàng với API

    print("\n" + "="*70)
    print("HOÀN THÀNH SCRAPING!")
    print(f"   • Thành công: {success} job")
    print(f"   • Thất bại:  {failed} job")
    print(f"   • Tổng mới lần này: {len(all_new_jobs)}")
    print(f"   • CSV: {CSV_FILE}")
    print(f"   • Summary: {SUMMARY_FILE}")
    print("="*70)