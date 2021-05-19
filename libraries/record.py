import logging
import logging.config
import re
import xml.etree.ElementTree as ET
from typing import Optional, Tuple

logging.config.fileConfig('logging.conf', disable_existing_loggers=False)
logger = logging.getLogger(__name__)

oclc_org_code_prefix = '(OCoLC)'
oclc_org_code_prefix_len = len(oclc_org_code_prefix)
valid_oclc_number_prefixes = {'ocm', 'ocn', 'on'}
valid_oclc_number_prefixes_str = f"If present, the OCLC number prefix " \
    f"must be one of the following: {', '.join(valid_oclc_number_prefixes)}"


def extract_oclc_num_from_subfield_a(
        subfield_a: str,
        field_035_element_index: int,
        found_error_in_record: bool
        ) -> Optional[Tuple[str, str, bool, bool, bool]]:
    # Extract the OCLC number itself
    oclc_num_without_org_code_prefix = \
        subfield_a[oclc_org_code_prefix_len:].rstrip()

    match_on_first_digit = re.search(r'\d',
        oclc_num_without_org_code_prefix)

    if oclc_num_without_org_code_prefix == '':
        oclc_num_without_org_code_prefix = \
            f'<nothing after {oclc_org_code_prefix}>'

    extracted_oclc_num = \
        oclc_num_without_org_code_prefix
    extracted_oclc_num_prefix = ''

    if match_on_first_digit is None:
        logger.debug(f'This OCLC number has no digits: ' \
            f'{subfield_a}')
    else:
        extracted_oclc_num = \
            oclc_num_without_org_code_prefix[
                match_on_first_digit.start():]
        extracted_oclc_num_prefix = \
            oclc_num_without_org_code_prefix[
                :match_on_first_digit.start()]

    found_valid_oclc_prefix = True
    found_valid_oclc_num = True

    # Check for invalid prefix
    if len(extracted_oclc_num_prefix) > 0:
        logger.debug(f'035 field #{field_035_element_index + 1}, extracted ' \
            f'OCLC number prefix: {extracted_oclc_num_prefix}')

        if (extracted_oclc_num_prefix not in
            valid_oclc_number_prefixes):
            found_valid_oclc_prefix = False

            logger.debug(f"'{extracted_oclc_num_prefix}' is " \
                f"an invalid OCLC number prefix. " \
                f"{valid_oclc_number_prefixes_str}")

            # Include invalid prefix with OCLC number
            extracted_oclc_num = (
                extracted_oclc_num_prefix
                + extracted_oclc_num)

    # Check for invalid number
    found_valid_oclc_num = \
        extracted_oclc_num.isdigit()

    # Remove leading zeros if extracted OCLC number is valid
    if found_valid_oclc_prefix and found_valid_oclc_num:
        try:
            extracted_oclc_num = \
                remove_leading_zeros(
                    extracted_oclc_num)
        except ValueError as value_err:
            logger.exception(f"A ValueError occurred when " \
                f"trying to remove leading zeros from " \
                f"'{extracted_oclc_num}', which " \
                f"was extracted from an 035 $a field of " \
                f"MMS ID '{mms_id}'. To remove leading " \
                f"zeros, the extracted OCLC number cannot " \
                f"contain a decimal point or any other " \
                f"non-digit character. Error message: " \
                f"{value_err}")
            found_error_in_record = True
    else:
        if not found_valid_oclc_num:
            logger.debug(f"'{extracted_oclc_num}'" \
                f" is an invalid OCLC number (because it " \
                f"contains at least one non-digit character).")
        found_error_in_record = True

    logger.debug(f'035 field #{field_035_element_index + 1}, extracted OCLC ' \
        f'number: {extracted_oclc_num}')

    return (oclc_num_without_org_code_prefix,
        extracted_oclc_num,
        found_valid_oclc_prefix,
        found_valid_oclc_num,
        found_error_in_record)


def get_subfield_a_with_oclc_num(
        field_035_element: ET.Element,
        field_035_element_index: int
        ) -> Optional[str]:
    subfield_a_element = \
        field_035_element.find('./subfield[@code="a"]')
    if subfield_a_element is None:
        logger.debug(f'035 field #{field_035_element_index + 1} has no ' \
            f'subfield a.')
        return None

    subfield_a = subfield_a_element.text
    logger.debug(f'035 field #{field_035_element_index + 1}, subfield a: ' \
        f'{subfield_a}')

    # Skip this 035 field if it's not an OCLC number
    if not subfield_a.startswith(oclc_org_code_prefix):
        return None

    return subfield_a


def remove_leading_zeros(string: str) -> str:
    """Removes leading zeros from the given string.

    Parameters
    ----------
    string: str
        The string to remove leading zeros from. This string must represent an
        integer value (i.e. it cannot contain a decimal point or any other
        non-digit character).

    Returns
    -------
    str
        The string without leading zeros
    """
    return str(int(string))
