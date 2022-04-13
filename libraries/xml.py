import logging
import requests
from typing import Callable
from xml.dom import minidom

logger = logging.getLogger(__name__)


def prettify_and_log_xml(
        xml_str: str,
        heading: str,
        logging_func: Callable[..., None] = logger.debug) -> bytes:
    xml_as_pretty_printed_bytes_obj = prettify(xml_str)

    log_xml_string(xml_as_pretty_printed_bytes_obj, heading, logging_func)

    return xml_as_pretty_printed_bytes_obj


def prettify(xml_str: str) -> bytes:
    xml_as_pretty_printed_bytes_obj = \
        minidom.parseString(xml_str).toprettyxml(indent='  ', encoding='UTF-8')

    return xml_as_pretty_printed_bytes_obj


def log_xml_string(
        xml_as_bytes_obj: bytes,
        heading: str,
        logging_func: Callable[..., None] = logger.debug) -> None:
    logging_func(f'{heading}:\n{xml_as_bytes_obj.decode("UTF-8")}')
