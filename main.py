import asyncio
from tortoise import Tortoise

# Configuration & Design Patterns
from src.core.config import settings
from src.core.factories.storage_factory import StorageFactory
from src.core.factories.browser_factory import BrowserFactory
from src.core.repository import RuleRepository, JobRepository
from src.core.recovery import AISelectorRecovery
from src.core.llm_provider import get_llm_provider
from src.core.queue import InMemoryQueue, RedisQueue
from src.adapters.proxy_adapter import DirectProxy

# Service & Events
from src.core.service import ScraperService
from src.core.events import event_dispatcher
from src.plugins.linkedin_plugin import LinkedinPlugin
from src.utils.logger import setup_logger, logger
from src.utils.seeding.rules import seed_all_rules

# Initialize Loggers
setup_logger()

async def on_job_processed(data):
    """Event Listener: Một ví dụ về Observer Pattern"""
    logger.info("event.handler.job_processed", 
                job_title=data['title'], 
                is_new=data['is_new'])

async def init_db():
    """Tách biệt logic khởi tạo DB"""
    await Tortoise.init(
        db_url=settings.DATABASE_URL,
        modules={'models': ['src.models.orm_models']}
    )
    conn = Tortoise.get_connection("default")
    await conn.execute_script("CREATE SCHEMA IF NOT EXISTS jobranking;")
    
    # Reset schema if columns changed (Uncomment below if you need to recreate tables)
    # await conn.execute_script("DROP SCHEMA IF EXISTS jobranking CASCADE; CREATE SCHEMA jobranking;")
    
    await Tortoise.generate_schemas()
    await seed_all_rules()

async def main():
    """
    Điểm khởi đầu của ứng dụng (High-level Orchestration).
    File này giờ đây cực kỳ sạch sẽ nhờ áp dụng các Design Pattern.
    """
    logger.info("app.start", status="initializing")

    try:
        await init_db()

        # 1. Khởi tạo Core thông qua Design Patterns
        storage = StorageFactory.create_storage()
        proxy = DirectProxy()
        browser = BrowserFactory.create_browser(proxy)
        
        llm = get_llm_provider({
            "type": settings.LLM_TYPE,
            "base_url": settings.OLLAMA_BASE_URL,
            "model": settings.OPENAI_MODEL if settings.LLM_TYPE == "openai" else settings.OLLAMA_MODEL,
            "api_key": settings.OPENAI_API_KEY
        })
        recovery = AISelectorRecovery(llm)
        
        # Repositories & Queue
        rule_repo = RuleRepository()
        job_repo = JobRepository()
        queue = InMemoryQueue()
        
        # 2. Service Layer & Events
        scraper_service = ScraperService(
            browser, storage, rule_repo, job_repo, recovery, queue
        )
        event_dispatcher.subscribe("job.processed", on_job_processed)
        
        # 3. Register Plugins & Run
        linkedin_plugin = LinkedinPlugin(browser, storage, rule_repo, recovery)
        scraper_service.register_plugin(linkedin_plugin)
        
        keywords = ["IT"]
        await scraper_service.run(keywords)

    except Exception as e:
        logger.error("app.fatal_error", error=str(e))
    finally:
        await Tortoise.close_connections()
        logger.info("app.finish")

if __name__ == "__main__":
    asyncio.run(main())
