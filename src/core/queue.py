from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
import asyncio
import redis.asyncio as redis
import json

class URLQueue(ABC):
    @abstractmethod
    async def push(self, item: Dict[str, Any], priority: int = 0):
        pass

    @abstractmethod
    async def pop(self) -> Optional[Dict[str, Any]]:
        pass

    @abstractmethod
    async def size(self) -> int:
        pass

class InMemoryQueue(URLQueue):
    def __init__(self):
        self.queue = asyncio.PriorityQueue()
        self.seen_urls = set()
        self.counter = 0

    async def push(self, item: Dict[str, Any], priority: int = 0):
        url = item.get("url")
        if url and url not in self.seen_urls:
            self.counter += 1
            # lower number = higher priority in PriorityQueue
            # Use counter to avoid dict comparison when priorities are equal
            await self.queue.put((priority, self.counter, item))
            self.seen_urls.add(url)

    async def pop(self) -> Optional[Dict[str, Any]]:
        if self.queue.empty():
            return None
        priority, count, item = await self.queue.get()
        return item

    async def size(self) -> int:
        return self.queue.qsize()

class RedisQueue(URLQueue):
    def __init__(self, redis_url: str, queue_name: str = "scraping_queue"):
        self.redis = redis.from_url(redis_url)
        self.queue_name = queue_name
        self.seen_set = f"{queue_name}:seen"

    async def push(self, item: Dict[str, Any], priority: int = 0):
        url = item.get("url")
        if not url: return
        
        # Check if seen
        is_new = await self.redis.sadd(self.seen_set, url)
        if is_new:
            # Use sorted set for priority if needed, or simple list
            await self.redis.lpush(self.queue_name, json.dumps(item))

    async def pop(self) -> Optional[Dict[str, Any]]:
        data = await self.redis.rpop(self.queue_name)
        if data:
            return json.loads(data)
        return None

    async def size(self) -> int:
        return await self.redis.llen(self.queue_name)
