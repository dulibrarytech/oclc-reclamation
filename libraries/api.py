import logging
import logging.config
import requests

logging.config.fileConfig('logging.conf', disable_existing_loggers=False)
logger = logging.getLogger(__name__)

def log_response_and_raise_for_status(
    response: requests.models.Response) -> None:
    logger.debug(f'API Response: {response}')
    logger.debug(f'Request URL: {response.url}')
    logger.debug(f'Status: {response.status_code}')
    logger.debug(f'Encoding: {response.encoding}')
    response.raise_for_status()
