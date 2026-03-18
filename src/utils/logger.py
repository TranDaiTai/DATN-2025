import structlog
import logging
import sys

def setup_logger():
    """Configures structured (JSON) logging for the application."""
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            structlog.processors.JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # Standard logging redirect to structlog
    logging.basicConfig(
        format="%(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("app.log", encoding="utf-8")
        ],
        level=logging.INFO,
    )
    from .logger import logger
    logger.info("logger.initialized", file="app.log")

logger = structlog.get_logger()
