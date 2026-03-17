from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional

class Settings(BaseSettings):
    """
    Quản lý cấu hình tập trung bằng Pydantic.
    Tự động đọc từ file .env và kiểm tra kiểu dữ liệu (Type-safety).
    """
    # Database
    DATABASE_URL: str = "postgres://postgres:123@localhost:5432/postgres"

    # LLM
    LLM_TYPE: str = "ollama"
    OLLAMA_BASE_URL: str = "http://localhost:11434"
    OLLAMA_MODEL: str = "llama3"
    OPENAI_API_KEY: Optional[str] = None
    OPENAI_MODEL: str = "gpt-4o"

    # Scraper
    HEADLESS: bool = True
    REQUEST_TIMEOUT: int = 30
    MAX_CONCURRENT_TASKS: int = 3
    DELAY_BETWEEN_REQUESTS: int = 2

    # S3 Storage (Optional)
    S3_BUCKET: Optional[str] = None
    S3_ACCESS_KEY: Optional[str] = None
    S3_SECRET_KEY: Optional[str] = None
    S3_REGION: str = "us-east-1"
    S3_ENDPOINT: Optional[str] = None

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

# Khởi tạo instance duy nhất (Singleton-like) để dùng toàn app
settings = Settings()
