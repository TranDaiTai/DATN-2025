from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import aiohttp
import json
from ..utils.exceptions import LLMProviderError
from ..utils.logger import logger

class LLMProvider(ABC):
    """
    Interface cho các nhà cung cấp mô hình ngôn ngữ lớn (LLM).
    Cho phép hệ thống chuyển đổi giữa API (OpenAI) và Self-hosted (Ollama) dễ dàng.
    """
    @abstractmethod
    async def chat_completion(self, prompt: str, system_prompt: str = "") -> str:
        pass

class OpenAIProvider(LLMProvider):
    def __init__(self, api_key: str, model: str = "gpt-4o"):
        self.api_key = api_key
        self.model = model
        self.url = "https://api.openai.com/v1/chat/completions"

    async def chat_completion(self, prompt: str, system_prompt: str = "") -> str:
        """Gửi yêu cầu đến OpenAI API"""
        logger.info("llm_provider.openai.request", model=self.model)
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        data = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt}
            ],
            "response_format": { "type": "json_object" }
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(self.url, headers=headers, json=data) as resp:
                    if resp.status != 200:
                        error_text = await resp.text()
                        logger.error("llm_provider.openai.error", status=resp.status, error=error_text)
                        raise LLMProviderError(f"OpenAI API error: {resp.status}")
                    result = await resp.json()
                    return result['choices'][0]['message']['content']
        except Exception as e:
            raise LLMProviderError(f"OpenAI connection failed: {str(e)}")

class OllamaProvider(LLMProvider):
    def __init__(self, base_url: str = "http://localhost:11434", model: str = "llama3"):
        self.base_url = f"{base_url}/api/chat"
        self.model = model

    async def chat_completion(self, prompt: str, system_prompt: str = "") -> str:
        """Gửi yêu cầu tới Ollama (Local/Self-hosted)"""
        logger.info("llm_provider.ollama.request", model=self.model, url=self.base_url)
        data = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt}
            ],
            "stream": False,
            "format": "json"
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(self.base_url, json=data) as resp:
                    if resp.status != 200:
                        logger.error("llm_provider.ollama.error", status=resp.status)
                        raise LLMProviderError(f"Ollama error: {resp.status}")
                    result = await resp.json()
                    return result['message']['content']
        except Exception as e:
            raise LLMProviderError(f"Ollama connection failed: {str(e)}")

def get_llm_provider(config: Dict[str, Any]) -> LLMProvider:
    """Factory method để khởi tạo LLM Provider dựa trên cấu hình config"""
    provider_type = config.get("type", "ollama")
    if provider_type == "openai":
        return OpenAIProvider(api_key=config["api_key"], model=config.get("model", "gpt-4o"))
    elif provider_type == "ollama":
        return OllamaProvider(base_url=config.get("base_url", "http://localhost:11434"), model=config.get("model", "llama3"))
    else:
        raise ValueError(f"Unsupported LLM provider: {provider_type}")
