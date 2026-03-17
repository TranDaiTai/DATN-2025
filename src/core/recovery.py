from bs4 import BeautifulSoup
import json
import re
from typing import Dict, Any, Optional
from .llm_provider import LLMProvider
from ..utils.logger import logger
from ..utils.exceptions import RecoveryFailedError

class AISelectorRecovery:
    """
    Module tự chữa lành (Self-healing).
    Khi các Selector cũ bị hỏng, module này sẽ gửi HTML sang AI để phân tích và tìm Selector mới.
    """
    def __init__(self, llm_provider: LLMProvider):
        self.llm_provider = llm_provider

    def _prune_html(self, html: str) -> str:
        """
        Cắt tỉa HTML để giảm số lượng Token gửi lên AI (tiết kiệm chi phí/tăng tốc độ).
        Xóa bỏ các thẻ không cần thiết như script, style, svg.
        """
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "svg", "path", "iframe", "footer", "nav"]):
            tag.decompose()
        
        # Chỉ lấy phần thân bài viết và giới hạn ký tự
        return soup.body.decode_contents()[:15000]

    async def recover_selector(self, site_name: str, field_name: str, html: str, last_known_selector: str = "") -> Optional[Dict[str, Any]]:
        """Gửi yêu cầu tới AI để tìm selector mới"""
        logger.info("ai_recovery.start", site=site_name, field=field_name)
        pruned_html = self._prune_html(html)
        
        system_prompt = (
            "You are an HTML extraction expert. A web scraper failed to extract a specific field. "
            "Return ONLY a JSON object. No explanation, no markdown."
        )
        
        prompt = f"""
Site: {site_name}
Field: {field_name}
Last working selector: {last_known_selector}

HTML Snippet:
{pruned_html}

Return a valid CSS or XPath selector for this field in JSON format:
{{
  "field": "{field_name}",
  "selector": "<your CSS or XPath selector>",
  "selector_type": "css" | "xpath",
  "confidence": 0.0-1.0,
  "reasoning": "<one sentence>"
}}
"""
        try:
            response_text = await self.llm_provider.chat_completion(prompt, system_prompt)
            # Trích xuất JSON từ phản hồi của AI (phòng trường hợp AI trả về text thừa)
            json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
            if json_match:
                result = json.loads(json_match.group(0))
                logger.info("ai_recovery.success", field=field_name, confidence=result.get('confidence'))
                return result
            
            raise RecoveryFailedError(f"AI returned invalid JSON: {response_text}")
        except Exception as e:
            logger.error("ai_recovery.failed", field=field_name, error=str(e))
            return None
