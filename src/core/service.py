from typing import List
from .browser import BrowserManager
from .storage import StorageManager
from .repository import RuleRepository, JobRepository
from .recovery import AISelectorRecovery
from .queue import URLQueue
from .events import event_dispatcher
from ..utils.logger import logger
from ..utils.exceptions import ScraperException
import asyncio

class ScraperService:
    """
    Orchestrator Service: Điều phối toàn bộ quy trình cào dữ liệu.
    Đây là "bộ não" điều khiển các module Core và Plugin.
    """
    def __init__(self, 
                 browser: BrowserManager,
                 storage: StorageManager,
                 rule_repo: RuleRepository,
                 job_repo: JobRepository,
                 recovery: AISelectorRecovery,
                 queue: URLQueue):
        self.browser = browser
        self.storage = storage
        self.rule_repo = rule_repo
        self.job_repo = job_repo
        self.recovery = recovery
        self.queue = queue
        self.active_plugins = []

    def register_plugin(self, plugin):
        """Đăng ký các Plugin (LinkedIn, TopCV...) vào hệ thống"""
        self.active_plugins.append(plugin)

    async def run(self, keywords: List[str]):
        """Bắt đầu quy trình cào dữ liệu toàn diện"""
        logger.info("service.run.start", plugins_count=len(self.active_plugins))
        
        try:
            await self.browser.start()
            
            # Bước 1: Thu thập danh sách từ tất cả các Plugin
            for plugin in self.active_plugins:
                listings = await plugin.crawl_listings(keywords)
                logger.info("service.run.plugin_results", plugin=plugin.__class__.__name__, count=len(listings))
                for item in listings:
                    await self.queue.push(item)
            
            queue_size = await self.queue.size()
            logger.info("service.run.queue_ready", total_items=queue_size)
            
            # Bước 2: Xử lý chi tiết từ Queue
            while await self.queue.size() > 0:
                item = await self.queue.pop()
                await self._process_single_job(item)
                
                # Nghỉ ngơi trốn tránh sự phát hiện (Politeness)
                await asyncio.sleep(2)

        except Exception as e:
            logger.error("service.run.failed", error=str(e))
            raise
        finally:
            await self.browser.close()
            logger.info("service.run.finish")

    async def _process_single_job(self, item: dict):
        """Logic xử lý nội bộ cho từng Job đơn lẻ"""
        logger.info("service.process_job.start", url=item.get('url'))
        # Tìm plugin phù hợp cho trang này
        plugin = next((p for p in self.active_plugins if p.__class__.__name__.lower().startswith(item['site_name'])), None)
        
        if not plugin:
            logger.warning("service.process_job.no_plugin", site_name=item.get('site_name'))
            return

        raw_data = await plugin.extract_details(item)
        logger.info("service.process_job.extracted", success=bool(raw_data), url=item.get('url'))
        if raw_data:
            # Lưu vào Database thông qua Repository
            job_obj, created = await self.job_repo.update_or_create_job(raw_data)
            await self.job_repo.add_source(job_obj, raw_data['site_name'], raw_data['url'])
            
            # Phát sự kiện (Event) khi xử lý xong một Job
            await event_dispatcher.emit("job.processed", {
                "id": str(job_obj.id),
                "title": job_obj.job_title,
                "is_new": created
            })
