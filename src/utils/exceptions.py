class ScraperException(Exception):
    """Base exception for all scraper errors."""
    def __init__(self, message: str, metadata: dict = None):
        super().__init__(message)
        self.metadata = metadata or {}

class BrowserException(ScraperException):
    """Exceptions related to browser automation."""
    pass

class BrowserTimeoutError(BrowserException):
    """Raised when a page load or selector wait times out."""
    pass

class CaptchaDetectedError(BrowserException):
    """Raised when a CAPTCHA is detected on the page."""
    pass

class DatabaseError(ScraperException):
    """Exceptions related to database operations."""
    pass

class LLMProviderError(ScraperException):
    """Exceptions related to LLM API or local server."""
    pass

class RecoveryFailedError(ScraperException):
    """Raised when AI recovery fails to find a valid selector."""
    pass
