import logging
import requests

logger = logging.getLogger(__name__)


def log_response_and_raise_for_status(
        response: requests.models.Response) -> None:
    logger.debug(f'API response details:\n' \
        f'\t- URL: {response.url}\n' \
        f'\t- HTTP status code: {response.status_code}\n' \
        f'\t- Encoding: {response.encoding}')

    response.raise_for_status()
