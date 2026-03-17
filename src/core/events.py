from typing import Callable, Dict, List, Any
import asyncio
from ..utils.logger import logger

class EventDispatcher:
    """
    Observer Pattern: Giúp các thành phần trong hệ thống giao tiếp với nhau mà không cần biết nhau.
    Ví dụ: Khi cào xong 1 Job, Dispatcher sẽ thông báo cho Logger, Emailer, hoặc Search Engine.
    """
    def __init__(self):
        self._listeners: Dict[str, List[Callable]] = {}

    def subscribe(self, event_type: str, listener: Callable):
        """Đăng ký một hàm xử lý cho một loại sự kiện cụ thể"""
        if event_type not in self._listeners:
            self._listeners[event_type] = []
        self._listeners[event_type].append(listener)

    async def emit(self, event_type: str, data: Any):
        """Phát đi một sự kiện kèm theo dữ liệu"""
        if event_type in self._listeners:
            tasks = [asyncio.create_task(listener(data)) for listener in self._listeners[event_type]]
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
        logger.debug("event.emitted", event=event_type)

# Singleton Instance
event_dispatcher = EventDispatcher()
