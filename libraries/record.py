import logging
import re
import string
import xml.etree.ElementTree as ET
from typing import NamedTuple, Optional, Tuple

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
        found_error_in_record: bool
        ) -> Tuple[str, str, bool, bool, bool]:
    """Extracts OCLC number from the given 035 $a field and checks if it's valid.

    Parameters
    ----------
    subfield_a_str: str
        The subfield $a to extract from
    field_035_element_index: int
        The index of the 035 field containing this subfield $a
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
    oclc_num_without_org_code_prefix = \
        remove_oclc_org_code_prefix(subfield_a_str)

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


def get_valid_record_identifier(record_identifier: str,
        identifier_name: str) -> str:
    """Checks the validity of the given record identifier and returns it.

    An identifier is considered invalid if it is empty or contains any non-digit
    character (after removing leading or trailing whitespace).

    If valid, returns the record identifier with any leading or trailing
    whitespace removed.
    If invalid, raises AssertionError.

    Parameters
    ----------
    record_identifier: str
        The record identifier to check
    identifier_name: str
        The name of the record identifier (e.g. 'OCLC number')

    Returns
    -------
    str
        The record identifier with whitespace removed from the beginning and end
        of the string

    Raises
    ------
    AssertionError
        If the record identifier is invalid (i.e. it is empty or contains any
        non-digit character)
    """
    empty_identifier_error_msg = (f"Invalid {identifier_name}: "
        f"'{record_identifier}'. It cannot be empty.")

    assert record_identifier is not None, empty_identifier_error_msg

    record_identifier = record_identifier.strip()

    assert len(record_identifier) > 0, empty_identifier_error_msg
    assert record_identifier.isdigit(), (f"Invalid {identifier_name}: "
        f"'{record_identifier}' must contain only digits.")

    return record_identifier


def is_valid_record_identifier(record_identifier: str,
        identifier_name: str) -> bool:
    """Determines whether the given record identifier is valid.

    Before the validity check, the record identifier is stripped of any leading
    or trailing whitespace.

    - Unless otherwise specified, an identifier is considered invalid if it
    contains any non-digit character.
    - However, ISBNs and ISSNs may end in the letter 'X', case-insensitive (but
    must otherwise contain only digits).

    Parameters
    ----------
    record_identifier: str
        The record identifier to check
    identifier_name: str
        The name of the record identifier (e.g. 'isbn')

    Returns
    -------
    bool
        True if the record identifier is valid; otherwise, False
    """
    record_identifier = record_identifier.strip()

    invalid_identifier_error_msg = (f"Invalid {identifier_name}: "
        f"'{record_identifier}'")

    if record_identifier == '':
        logger.error(f"{invalid_identifier_error_msg}. It cannot be empty.")
        return False

    if record_identifier.isdigit():
        return True

    # Consider as valid any ISBN or ISSN with a single trailing 'X' character
    # (case-insensitive).
    if identifier_name.lower().startswith(('isbn', 'issn')):
        if (record_identifier.endswith(('X', 'x'))
                and record_identifier[:-1].isdigit()):
            return True

        logger.error(f"{invalid_identifier_error_msg}. Must contain only "
            f"digits with or without a single trailing 'X'.")
        return False

    # All validity checks failed, so record identifier is invalid
    logger.error(f"{invalid_identifier_error_msg}. Must contain only "
        f"digits.")
    return False


def remove_leading_zeros(orig_str: str) -> str:
    """Removes leading zeros from the given string, if applicable.

    Parameters
    ----------
    orig_str: str
        The string to remove leading zeros from. This string must represent an
        integer value (i.e. it cannot contain a decimal point or any other
        non-digit character).

    Returns
    -------
    str
        The string without leading zeros
    """
    return str(int(orig_str))


def remove_oclc_org_code_prefix(full_oclc_string: str) -> str:
    """Removes the OCLC org code prefix from the given string, if applicable.

    Also strips whitespace from the end of the string, if applicable.

    Parameters
    ----------
    full_oclc_string: str
        The string containing the full OCLC number

    Returns
    -------
    str
        The string without the OCLC org code prefix
    """
    return (full_oclc_string[len(oclc_org_code_prefix):].rstrip()
        if full_oclc_string.startswith(oclc_org_code_prefix)
        else full_oclc_string.rstrip())


def remove_punctuation_and_spaces(orig_str: str) -> str:
    """Removes punctuation and spaces from the given string, if applicable.

    Also converts uppercase characters into lowercase.

    Parameters
    ----------
    orig_str: str
        The string to remove punctuation and spaces from (and make lowercase)

    Returns
    -------
    str
        The lowercase string without punctuation and spaces
    """
    return orig_str.translate(
        str.maketrans('', '', string.punctuation + ' ')).lower()


def split_and_join_valid_record_identifiers(
        str_with_record_identifiers: str,
        identifier_name: str,
        split_separator: Optional[str] = None,
        join_separator: str = '|') -> str:
    """Splits and then joins all valid record identifiers from the given string.

    1) The given string is split based on the split_separator. Each resulting
    element is stripped of any leading or trailing whitespace.
    2) All invalid identifiers are removed.
    3) The remaining identifiers are joined based on the join_separator.

    Parameters
    ----------
    str_with_record_identifiers: str
        The string containing record identifiers that needs to be split
    identifier_name: str
        The name of the record identifier (e.g. 'isbn')
    split_separator: Optional[str], default is None
        The separator to use when splitting the string
    join_separator: str, default is '|'
        The separator to use when joining the string

    Returns
    -------
    str
        A string joining all valid record identifiers
    """
    str_with_record_identifiers = str_with_record_identifiers.strip()

    if str_with_record_identifiers == '':
        return ''

    identifiers_list = None
    if split_separator is None:
        identifiers_list = (
            element.strip() for element in str_with_record_identifiers.split())
    else:
        identifiers_list = (
            element.strip() for element in str_with_record_identifiers.split(
                split_separator))

    identifiers_list_as_str = join_separator.join(filter(
        lambda identifier: is_valid_record_identifier(
            identifier,
            identifier_name),
        identifiers_list))
    logger.debug(f"String after joining valid {identifier_name} values: "
        f"'{identifiers_list_as_str}'")

    return identifiers_list_as_str
