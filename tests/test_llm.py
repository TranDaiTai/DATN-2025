import pytest
from unittest.mock import AsyncMock, MagicMock
from src.core.llm_provider import OpenAIProvider, OllamaProvider, get_llm_provider
from src.utils.exceptions import LLMProviderError

@pytest.mark.asyncio
async def test_llm_factory():
    """Kiểm tra Factory method tạo đúng đối tượng LLM"""
    config_ollama = {"type": "ollama", "base_url": "http://localhost:11434"}
    provider = get_llm_provider(config_ollama)
    assert isinstance(provider, OllamaProvider)
    
    config_openai = {"type": "openai", "api_key": "test_key"}
    provider = get_llm_provider(config_openai)
    assert isinstance(provider, OpenAIProvider)

@pytest.mark.asyncio
async def test_ollama_error_handling(monkeypatch):
    """Kiểm tra xử lý lỗi khi Connection tới Ollama thất bại"""
    provider = OllamaProvider(base_url="http://invalid_url")
    
    # Giả lập lỗi kết nối
    with pytest.raises(LLMProviderError):
        await provider.chat_completion("hello")
