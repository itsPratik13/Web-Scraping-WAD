import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def log_info(message):
    """Log information messages."""
    logger.info(message)

def log_error(message):
    """Log error messages."""
    logger.error(message)
