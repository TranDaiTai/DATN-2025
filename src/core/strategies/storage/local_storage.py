import os
import hashlib
import json
import datetime
from typing import Dict, Any
from ...interfaces.core_interfaces import StorageStrategy
from ....utils.logger import logger

class LocalStorageStrategy(StorageStrategy):
    def __init__(self, base_dir: str = "data"):
        self.base_dir = base_dir
        self.html_dir = os.path.join(base_dir, "html")
        self.output_dir = os.path.join(base_dir, "output")
        os.makedirs(self.html_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)

    async def save_html(self, html: str, url: str) -> str:
        url_hash = hashlib.md5(url.encode()).hexdigest()
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{url_hash}_{timestamp}.html"
        filepath = os.path.join(self.html_dir, filename)
        
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(html)
        
        logger.info("storage.local.save_html", filename=filename)
        return filename

    async def save_json(self, data: Dict[str, Any], filename: str):
        filepath = os.path.join(self.output_dir, filename)
        with open(filepath, "a", encoding="utf-8") as f:
            f.write(json.dumps(data, ensure_ascii=False) + "\n")
        logger.info("storage.local.save_json", filename=filename)
