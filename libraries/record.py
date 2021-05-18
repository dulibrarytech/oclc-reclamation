import logging
import logging.config
import xml.etree.ElementTree as ET
from typing import Optional, Tuple

logging.config.fileConfig('logging.conf', disable_existing_loggers=False)
logger = logging.getLogger(__name__)


def extract_oclc_num_from_subfield_a(
        subfield_a_element_with_oclc_num: ET.Element
        ) -> Tuple[str, str, bool, bool]:
    pass


def get_subfield_a_element_with_oclc_num(field_035_element: ET.Element
        ) -> Optional[ET.Element]:
    pass
