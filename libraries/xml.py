import logging
import requests
from xml.dom import minidom

logger = logging.getLogger(__name__)


def prettify_and_log_xml(
        xml_str: str,
        heading: str) -> bytes:
    xml_as_pretty_printed_bytes_obj = prettify(xml_str)

    log_xml_string(xml_as_pretty_printed_bytes_obj, heading)

    return xml_as_pretty_printed_bytes_obj


def prettify(xml_str: str) -> bytes:
    xml_as_pretty_printed_bytes_obj = \
        minidom.parseString(xml_str).toprettyxml(indent='  ', encoding='UTF-8')

    return xml_as_pretty_printed_bytes_obj


def log_xml_string(xml_as_bytes_obj: bytes, heading: str) -> None:
    logger.debug(f'{heading}:\n{xml_as_bytes_obj.decode("UTF-8")}')
