import logging
import logging.config
import re
import xml.etree.ElementTree as ET
from typing import NamedTuple, Optional, Tuple

logging.config.fileConfig('logging.conf', disable_existing_loggers=False)
logger = logging.getLogger(__name__)

oclc_org_code_prefix = '(OCoLC)'
traditional_oclc_number_prefixes = ('ocm', 'ocn', 'on')
# Include '|a' as a valid OCLC number prefix (in addition to the traditional
# prefixes). Doing so will prevent certain Alma records from appearing in the
# records_with_errors CSV files.
valid_oclc_number_prefixes = {'|a'}.union(traditional_oclc_number_prefixes)
valid_oclc_number_prefixes_str = (f"If present, the OCLC number prefix must "
    f"be one of the following: {', '.join(valid_oclc_number_prefixes)}")
subfield_a_disclaimer = ('if an 035 field contains multiple $a values, then '
    'only its first $a value is listed here')


class Subfield_a(NamedTuple):
    """Data returned by the get_subfield_a_with_oclc_num function.

    Fields
    ------
    string_with_oclc_num: Optional[str]
        The subfield $a string (provided that the 035 field contains at least
        one subfield $a value and that it is an OCLC number); otherwise, None.
        If there are multiple subfield $a values, then only the first $a value
        is included (provided it is an OCLC number).
    subfield_a_count: int
        Number of subfield $a values in the 035 field
    error_msg: Optional[str]
        Message explaining the error found in the 035 field, if applicable;
        otherwise, None
    """
    string_with_oclc_num: Optional[str]
    subfield_a_count: int
    error_msg: Optional[str]


def extract_oclc_num_from_subfield_a(
        subfield_a_str: str,
        field_035_element_index: int,
        mms_id: str,
        found_error_in_record: bool
        ) -> Tuple[str, str, bool, bool, bool]:
    """Checks the given 035 field for a subfield $a containing an OCLC number.

    Parameters
    ----------
    subfield_a_str: str
        The subfield $a to extract from
    field_035_element_index: int
        The index of the 035 field containing this subfield $a
    mms_id: str
        The MMS ID of the record
    found_error_in_record: bool
        True if an error has been found in this record; otherwise, False

    Returns
    -------
    Tuple[str, str, bool, bool, bool]
        Tuple with data about the subfield $a extraction. Includes the
        following fields: oclc_num_without_org_code_prefix, extracted_oclc_num,
        found_valid_oclc_prefix, found_valid_oclc_num, found_error_in_record
    """
    # Extract the OCLC number itself
    oclc_num_without_org_code_prefix = (
        subfield_a_str[len(oclc_org_code_prefix):].rstrip()
        if subfield_a_str.startswith(oclc_org_code_prefix)
        else subfield_a_str.rstrip())

    match_on_first_digit = re.search(r'\d', oclc_num_without_org_code_prefix)

    if oclc_num_without_org_code_prefix == '':
        oclc_num_without_org_code_prefix = \
            f'<nothing after {oclc_org_code_prefix}>'

    extracted_oclc_num = oclc_num_without_org_code_prefix
    extracted_oclc_num_prefix = ''

    if match_on_first_digit is None:
        logger.debug(f'This OCLC number has no digits: {subfield_a_str}')
    else:
        extracted_oclc_num = \
            oclc_num_without_org_code_prefix[match_on_first_digit.start():]
        extracted_oclc_num_prefix = \
            oclc_num_without_org_code_prefix[:match_on_first_digit.start()]

    found_valid_oclc_prefix = True
    found_valid_oclc_num = True

    # Check for invalid number
    if not extracted_oclc_num.isdigit():
        found_valid_oclc_num = False
        # Consider as valid any number with a single trailing # character
        if extracted_oclc_num.endswith('#'):
            extracted_oclc_num_without_final_char = extracted_oclc_num[:-1]
            if extracted_oclc_num_without_final_char.isdigit():
                found_valid_oclc_num = True
                extracted_oclc_num = extracted_oclc_num_without_final_char

    # Check for invalid prefix
    if len(extracted_oclc_num_prefix) > 0:
        logger.debug(f'035 field #{field_035_element_index + 1}, extracted '
            f'OCLC number prefix: {extracted_oclc_num_prefix}')

        if extracted_oclc_num_prefix not in valid_oclc_number_prefixes:
            found_valid_oclc_prefix = False

            logger.debug(f"'{extracted_oclc_num_prefix}' is an invalid OCLC "
                f"number prefix. {valid_oclc_number_prefixes_str}")

            # Include invalid prefix with OCLC number
            extracted_oclc_num = extracted_oclc_num_prefix + extracted_oclc_num

    # Delete after testing
    logger.debug(f'{mms_id=}')
    logger.debug(f'{type(mms_id)=}')

    # Remove leading zeros if extracted OCLC number is valid
    if found_valid_oclc_prefix and found_valid_oclc_num:
        extracted_oclc_num = remove_leading_zeros(extracted_oclc_num)
    else:
        if not found_valid_oclc_num:
            logger.debug(f"'{extracted_oclc_num}' is an invalid OCLC number "
                f"(because it contains at least one non-digit character that "
                f"is not a single trailing '#' character).")
        found_error_in_record = True

    logger.debug(f'035 field #{field_035_element_index + 1}, extracted OCLC '
        f'number: {extracted_oclc_num}')

    return (oclc_num_without_org_code_prefix,
        extracted_oclc_num,
        found_valid_oclc_prefix,
        found_valid_oclc_num,
        found_error_in_record)


def get_subfield_a_with_oclc_num(
        field_035_element: ET.Element,
        field_035_element_index: int
        ) -> Subfield_a:
    """Checks the given 035 field for a subfield $a containing an OCLC number.

    If there are multiple subfield $a values, then only the first $a value is
    returned (provided it is an OCLC number) as part of the Subfield_a
    NamedTuple.

    Parameters
    ----------
    field_035_element: ET.Element
        The 035 field to check
    field_035_element_index: int
        The 035 field's index

    Returns
    -------
    Subfield_a
        NamedTuple with data about the subfield a value(s). Includes the
        following fields: string_with_oclc_num, subfield_a_count,
        error_msg
    """

    subfield_a_elements = field_035_element.findall('./subfield[@code="a"]')
    subfield_a_strings = list()

    for subfield_a_element_index, subfield_a_element in enumerate(
            subfield_a_elements, start=1):
        subfield_a_text = ('<empty>' if subfield_a_element.text is None
            else subfield_a_element.text)
        subfield_a_strings.append(subfield_a_text)
        logger.debug(f'035 field #{field_035_element_index + 1}, subfield a '
            f'#{subfield_a_element_index}: {subfield_a_text}')

    subfield_a_with_oclc_num = None
    subfield_a_count = len(subfield_a_strings)
    error_msg = None

    if subfield_a_count == 0:
        error_msg = (f'035 field #{field_035_element_index + 1} has no $a '
            f'value')
    else:
        # Check whether first subfield $a value is an OCLC number
        accepted_prefixes = tuple(
            {oclc_org_code_prefix}.union(traditional_oclc_number_prefixes))
        if subfield_a_strings[0].startswith(accepted_prefixes):
            subfield_a_with_oclc_num = subfield_a_strings[0]

        if subfield_a_count > 1:
            error_msg = (f'035 field #{field_035_element_index + 1} has '
                f'multiple $a values: {", ".join(subfield_a_strings)}')

    if error_msg is not None:
        logger.debug(error_msg)

    return Subfield_a(subfield_a_with_oclc_num,
        subfield_a_count,
        error_msg)


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
