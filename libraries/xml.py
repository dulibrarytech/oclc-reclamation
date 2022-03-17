import logging
import requests
from xml.dom import minidom

logger = logging.getLogger(__name__)


def prettify_and_log_xml(
        response: requests.models.Response,
        heading: str) -> bytes:
    xml_as_pretty_printed_bytes_obj = prettify(response)

    log_xml_string(xml_as_pretty_printed_bytes_obj, heading)

    return xml_as_pretty_printed_bytes_obj


def prettify(response: requests.models.Response) -> bytes:
    xml_as_pretty_printed_bytes_obj = minidom.parseString(
        response.text).toprettyxml(indent='  ', encoding='UTF-8')

    return xml_as_pretty_printed_bytes_obj


def log_xml_string(xml_as_bytes_obj: bytes, heading: str) -> None:
    logger.debug(f'{heading}:\n{xml_as_bytes_obj.decode("UTF-8")}')
