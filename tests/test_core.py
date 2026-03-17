import pytest
import asyncio
from src.core.queue import InMemoryQueue

@pytest.mark.asyncio
async def test_queue_flow():
    """Kiểm tra luồng hoạt động của Queue: Push -> Size -> Pop"""
    queue = InMemoryQueue()
    
    # Test push
    await queue.push({"id": 1, "url": "http://test.com"})
    assert await queue.size() == 1
    
    # Test pop
    item = await queue.pop()
    assert item["id"] == 1
    assert await queue.size() == 0
    
    # Test pop empty
    item_empty = await queue.pop()
    assert item_empty is None
