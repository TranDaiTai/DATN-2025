import asyncio
from dotenv import load_dotenv
from tortoise import Tortoise

# Nạp cấu hình chuyên nghiệp từ Pydantic Settings
from src.core.config import settings

# Các thành phần lõi
from src.core.browser import BrowserManager
from src.core.storage import StorageManager
from src.core.repository import RuleRepository, JobRepository
from src.core.recovery import AISelectorRecovery
from src.core.llm_provider import get_llm_provider
from src.core.queue import InMemoryQueue, RedisQueue
from src.adapters.proxy_adapter import DirectProxy
from src.adapters.s3_adapter import S3StorageAdapter
from src.core.service import ScraperService
from src.core.events import event_dispatcher
from src.plugins.linkedin_plugin import LinkedinPlugin
from src.utils.logger import setup_logger, logger

# Khởi tạo logger có cấu trúc (JSON)
setup_logger()

async def on_job_processed(data):
    """Event Listener: Một ví dụ về Observer Pattern"""
    logger.info("event.handler.job_processed", 
                job_title=data['title'], 
                is_new=data['is_new'])

async def main():
    """
    Điểm khởi đầu của ứng dụng (High-level Orchestration).
    File này giờ đây cực kỳ sạch sẽ nhờ áp dụng các Design Pattern.
    """
    logger.info("app.start", status="initializing")

    try:
        # 1. Khởi tạo Database (ORM)
        await Tortoise.init(
            db_url=settings.DATABASE_URL,
            modules={'models': ['src.models.orm_models']}
        )
        await Tortoise.generate_schemas()
        
        # 2. Khởi tạo các thành phần Adapter & Core
        s3_adapter = None
        if settings.S3_BUCKET:
            s3_adapter = S3StorageAdapter(
                bucket_name=settings.S3_BUCKET,
                access_key=settings.S3_ACCESS_KEY,
                secret_key=settings.S3_SECRET_KEY,
                region=settings.S3_REGION,
                endpoint_url=settings.S3_ENDPOINT
            )

        storage = StorageManager(s_3_adapter=s3_adapter)
        proxy = DirectProxy()
        browser = BrowserManager(proxy)
        llm = get_llm_provider({
            "type": settings.LLM_TYPE,
            "base_url": settings.OLLAMA_BASE_URL,
            "model": settings.OLLAMA_MODEL,
            "api_key": settings.OPENAI_API_KEY
        })
        recovery = AISelectorRecovery(llm)
        
        # Repository Pattern
        rule_repo = RuleRepository()
        job_repo = JobRepository()
        
        # Queue (Có thể đổi sang RedisQueue dễ dàng)
        queue = InMemoryQueue()
        
        # 3. Khởi tạo Service Layer (Orchestrator)
        scraper_service = ScraperService(
            browser, storage, rule_repo, job_repo, recovery, queue
        )
        
        # Đăng ký các sự kiện (Observer Pattern)
        event_dispatcher.subscribe("job.processed", on_job_processed)
        
        # 4. Cắm Plugin và Chạy
        linkedin_plugin = LinkedinPlugin(browser, storage, rule_repo, recovery)
        scraper_service.register_plugin(linkedin_plugin)
        
        # Chạy quy trình thu thập và trích xuất
        keywords = ["Python Developer", "ReactJS"]
        await scraper_service.run(keywords)

    except Exception as e:
        logger.error("app.fatal_error", error=str(e))
    finally:
        await Tortoise.close_connections()
        logger.info("app.finish")

if __name__ == "__main__":
    asyncio.run(main())
