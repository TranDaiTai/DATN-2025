# 🏗️ Source Code Architecture (Kiến trúc Mã nguồn)

Thư mục `src/` chứa toàn bộ logic cốt lõi của hệ thống **Advanced Web Scraper**. Mã nguồn được thiết kế theo hướng module hóa, áp dụng các Clean Architecture và Design Patterns để đảm bảo tính mở rộng và bảo trì dễ dàng cho Đồ án Tốt nghiệp.

## 📂 Cấu trúc thư mục (Directory Structure)

```text
src/
├── adapters/        # Lớp tương tác với các dịch vụ bên ngoài (S3, Proxy, Captcha)
├── core/            # Logic cốt lõi của hệ thống (Browser, Rules, Recovery, Config)
├── models/          # Định nghĩa Database Models (Tortoise ORM)
├── plugins/         # Chứa các Plugin riêng biệt cho từng trang web (LinkedIn, ...)
└── utils/           # Các công cụ hỗ trợ (Logger, Exceptions, Helpers)
```

## 🛡️ Các tính năng nổi bật (Key Features)

-   **Self-healing (Tự chữa lành)**: Khi cấu trúc HTML của trang web thay đổi làm hỏng Selector cũ, module `AISelectorRecovery` sẽ tự động sử dụng LLM để tìm kiếm Selector mới và cập nhật vào Database.
-   **Anti-Detection**: Sử dụng `playwright-stealth` và `ProxyAdapter` để vượt qua các cơ chế chống cào (anti-bot) của website.
-   **Deduplication (Khử trùng lặp)**: Sử dụng MD5 hash để đảm bảo mỗi tin tuyển dụng chỉ được lưu duy nhất một bản ghi cho dù thu thập từ nhiều nguồn khác nhau.
-   **Structured Logging**: Ghi log dưới dạng JSON thông qua `structlog`, sẵn sàng cho việc tích hợp vào các hệ thống quan sát dữ liệu tập trung.

## 🚀 Hướng dẫn phát triển (Development Guide)

Để thêm một trang web mới vào hệ thống:
1. Tạo một file mới trong `src/plugins/[ten_trang]_plugin.py`.
2. Kế thừa lớp `BaseSitePlugin`.
3. Triển khai 2 hàm chính: `crawl_listings` (lấy danh sách) và `extract_details` (trích xuất chi tiết).
4. Khai báo plugin mới trong `main.py`.

---
*Ghi chú: Đây là mã nguồn phục vụ mục đích học thuật cho Đồ án Tốt nghiệp. Vui lòng tuân thủ `robots.txt` và các quy định pháp luật khi sử dụng thực tế.*
