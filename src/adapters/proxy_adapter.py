from abc import ABC, abstractmethod
from typing import Optional, Dict

class ProxyAdapter(ABC):
    @abstractmethod
    def get_proxy(self) -> Optional[Dict[str, str]]:
        pass

class DirectProxy(ProxyAdapter):
    def get_proxy(self) -> Optional[Dict[str, str]]:
        return None  # No proxy

class RotatingProxyAdapter(ProxyAdapter):
    def __init__(self, proxy_list: list):
        self.proxy_list = proxy_list
        self.index = 0

    def get_proxy(self) -> Optional[Dict[str, str]]:
        if not self.proxy_list:
            return None
        proxy = self.proxy_list[self.index % len(self.proxy_list)]
        self.index += 1
        return {"server": proxy}
