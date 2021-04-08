import argparse
import logging
import logging.config
import os
import pandas as pd
import re
import requests
import xml.etree.ElementTree as ET
from csv import writer
from dotenv import load_dotenv
from requests.exceptions import HTTPError
from typing import NamedTuple
from xml.dom import minidom

load_dotenv()

logging.config.fileConfig('logging.conf', disable_existing_loggers=False)
logger = logging.getLogger(__name__)

API_URL = os.getenv('API_URL')
API_KEY = os.getenv('API_KEY')
headers = {'Authorization': f'apikey {API_KEY}'}
params = {'view': 'full'}

class Record_confirmation(NamedTuple):
    """Details about a specific call to the update_alma_record function.

    Fields
    ------
    was_updated: bool
        True if the update_alma_record function call resulted in the record
        actually being updated; otherwise, False
    orig_oclc_nums: str
        A comma-separated listing of the original OCLC Number(s) from the Alma
        record's 035 $a field(s)
    """
    was_updated: bool
    orig_oclc_nums: str


def get_alma_record(mms_id: str) -> ET.Element:
    """GETs the Alma record with the given MMS ID.

    Sends a GET request to the Ex Libris Alma BIBs API:
    https://developers.exlibrisgroup.com/alma/apis/bibs/

    Parameters
    ----------
    mms_id: str
        The MMS ID of the Alma record

    Returns
    -------
    ET.Element
        The root element of the parsed XML tree
    """

    response = requests.get(f'{API_URL}{mms_id}', params=params,
        headers=headers, timeout=45)
    logger.debug(f'GET reponse: {response}')
    logger.debug(f'Request URL: {response.url}')
    logger.debug(f'Status: {response.status_code}')
    logger.debug(f'Encoding: {response.encoding}')
    response.raise_for_status()

    # Pretty-print XML response
    xml_as_pretty_printed_str = \
        minidom.parseString(response.text).toprettyxml(indent='  ',
        encoding='UTF-8')
    logger.debug(f'Original record:\n' \
        f'{xml_as_pretty_printed_str.decode("UTF-8")}')

    # Create XML file
    with open(f'xml/{mms_id}_original.xml', 'wb') as file:
        file.write(xml_as_pretty_printed_str)

    # Return root element of XML tree
    return ET.fromstring(response.text)


def update_alma_record(mms_id: str, oclc_num: str) -> Record_confirmation:
    """Updates the Alma record to have the given OCLC number (if needed).

    Compares all 035 fields containing an OCLC number (in the subfield $a) to
    the oclc_num parameter. If needed, updates the Alma record such that:
    - the record contains the given OCLC number (oclc_num)
    - any non-matching OCLC numbers from an 035 field (in the subfield $a) are
      moved to the 019 field (and that 035 field is removed, along with any
      data in its subfields)

    Updates the Alma record by sending a PUT request to the Ex Libris Alma
    BIBs API: https://developers.exlibrisgroup.com/alma/apis/bibs/

    Parameters
    ----------
    mms_id: str
        The MMS ID of the Alma record to be updated
    oclc_num: str
        The OCLC number that the Alma record should have in an 035 field

    Returns
    -------
    Record_confirmation
        NamedTuple with details about the update attempt. Includes the
        following fields: was_updated and orig_oclc_nums
    """

    logger.debug(f"Attempting to update MMS ID '{mms_id}'...")

    assert mms_id is not None and len(mms_id) > 0, f"Invalid MMS ID: " \
        f"'{mms_id}'. It cannot be empty."

    assert oclc_num is not None and len(oclc_num) > 0, f"Invalid OCLC " \
        f"number: '{oclc_num}'. It cannot be empty."

    # Make sure MMS ID contains numbers only.
    # Don't validate the length because "The MMS ID can be 8 to 19 digits long
    # (with the first two digits referring to the record type and the last four
    # digits referring to a unique identifier for the institution)".
    # Source: https://knowledge.exlibrisgroup.com/Alma/Product_Documentation/010Alma_Online_Help_(English)/120Alma_Glossary
    assert mms_id.isdigit(), f"Invalid MMS ID: '{mms_id}' must " \
        f"contain only digits."

    # Make sure OCLC number contains numbers only and has at least 8 digits
    assert oclc_num.isdigit(), f"Invalid OCLC number: '{oclc_num}' must " \
        f"contain only digits."

    oclc_num_len = len(oclc_num)
    assert oclc_num_len >= 8, f"Invalid OCLC number: '{oclc_num}' contains " \
        f"{oclc_num_len} digits. To be valid, it must contain 8 or more digits."

    # Create full OCLC number string based on length of oclc_num
    full_oclc_num = '(OCoLC)'
    if oclc_num_len == 8:
        full_oclc_num += f'ocm{oclc_num} '
    elif oclc_num_len == 9:
        full_oclc_num += f'ocn{oclc_num}'
    else:
        full_oclc_num += f'on{oclc_num}'

    logger.debug(f'Full OCLC number: {full_oclc_num}')

    # Access XML elements of Alma record
    root = get_alma_record(mms_id)
    record_element = root.find('./record')

    need_to_update_record = False
    oclc_nums_from_record = list()
    oclc_nums_for_019_field = set()
    found_035_field_with_current_oclc_num = False

    # Iterate over each 035 field
    for i, element in enumerate(
        record_element.findall('./datafield[@tag="035"]')):
        # Extract subfield a (which would contain the OCLC number if present)
        subfield_a = element.find('./subfield[@code="a"]').text
        logger.debug(f'035 field #{i + 1}, subfield a: {subfield_a}')

        # Skip this 035 field if it's not an OCLC number
        if not subfield_a.startswith('(OCoLC)'):
            continue

        # Extract the OCLC number itself
        match_on_first_digit = re.search(r'\d', subfield_a)

        # TO DO: Decide if this should be an assertion, which prevents further
        # processing of the record.
        assert match_on_first_digit, f'This record contains an 035 field ' \
            f'with an OCLC number that has no digits ({subfield_a}).'

        extracted_oclc_num_from_record = \
            subfield_a[match_on_first_digit.start():].strip()
        logger.debug(f'035 field #{i + 1}, extracted OCLC number: ' \
            f'{extracted_oclc_num_from_record}')

        oclc_nums_from_record.append(extracted_oclc_num_from_record)

        # Compare the extracted OCLC number to the current OCLC number
        extracted_oclc_num_matches_current_oclc_num = \
            extracted_oclc_num_from_record == oclc_num.strip()
        logger.debug(f'Does the extracted OCLC number ' \
            f'({extracted_oclc_num_from_record}) match the current OCLC ' \
            f'number ({oclc_num})? ' \
            f'{extracted_oclc_num_matches_current_oclc_num}')

        if (not extracted_oclc_num_matches_current_oclc_num or
            found_035_field_with_current_oclc_num):
            # This 035 field either contains an old OCLC number or is a
            # duplicate of another 035 field with the current OCLC number.
            # In either case, remove this 035 field.
            record_element.remove(element)

            if not extracted_oclc_num_matches_current_oclc_num:
                oclc_nums_for_019_field.add(extracted_oclc_num_from_record)

            need_to_update_record = True
        else:
            found_035_field_with_current_oclc_num = True

    oclc_nums_from_record_list_length = len(oclc_nums_from_record)
    oclc_nums_from_record_str = None
    if oclc_nums_from_record_list_length > 0:
        oclc_nums_from_record_str = ', '.join(oclc_nums_from_record)

    logger.debug(f'{oclc_nums_for_019_field=}')

    # Only add or edit the 019 field if oclc_nums_for_019_field set is non-empty
    if oclc_nums_for_019_field:
        # Search record for 019 field
        first_019_element = record_element.find('./datafield[@tag="019"]')

        # If the record has no 019 field, create one
        if not first_019_element:
            logger.debug(f'Original record does not have an 019 field.')
            first_019_element = ET.SubElement(record_element, 'datafield')
            first_019_element.set('ind1', ' ')
            first_019_element.set('ind2', ' ')
            first_019_element.set('tag', '019')

        first_019_element_as_str = \
            ET.tostring(first_019_element, encoding="unicode")
        logger.debug(f'First 019 element:\n{first_019_element_as_str}')

        # Add old OCLC numbers to 019 field
        for old_oclc_num in oclc_nums_for_019_field:
            sub_element = ET.SubElement(first_019_element, 'subfield')
            sub_element.set('code', 'a')
            sub_element.text = old_oclc_num

            first_019_element_as_str = \
                ET.tostring(first_019_element, encoding="unicode")
            logger.debug(f'First 019 element after adding {old_oclc_num}:\n' \
                f'{first_019_element_as_str}')

    if not found_035_field_with_current_oclc_num:
        # Create new 035 element with OCLC number
        new_035_element = ET.SubElement(record_element, 'datafield')
        new_035_element.set('ind1', ' ')
        new_035_element.set('ind2', ' ')
        new_035_element.set('tag', '035')
        sub_element = ET.SubElement(new_035_element, 'subfield')
        sub_element.set('code', 'a')
        sub_element.text = full_oclc_num

        new_035_element_as_str = \
            ET.tostring(new_035_element, encoding="unicode")
        logger.debug(f'New 035 element:\n{new_035_element_as_str}')

        need_to_update_record = True

    if need_to_update_record:
        # Send PUT request
        headers['Content-Type'] = 'application/xml'
        payload = ET.tostring(root, encoding='UTF-8')

        put_response = requests.put(f'{API_URL}{mms_id}', headers=headers,
            data=payload, timeout=45)
        logger.debug(f'PUT reponse: {put_response}')
        logger.debug(f'Request URL: {put_response.url}')
        logger.debug(f'Status: {put_response.status_code}')
        logger.debug(f'Encoding: {put_response.encoding}')
        put_response.raise_for_status()

        # Pretty-print XML response
        xml_as_pretty_printed_str = \
            minidom.parseString(put_response.text).toprettyxml(indent='  ',
            encoding='UTF-8')
        logger.debug(f'Modified record:\n' \
            f'{xml_as_pretty_printed_str.decode("UTF-8")}')

        # Create XML file
        with open(f'xml/{mms_id}_modified.xml', 'wb') as file:
            file.write(xml_as_pretty_printed_str)

        logger.debug(f"MMS ID '{mms_id}' has been updated.")
        return Record_confirmation(True, oclc_nums_from_record_str)

    logger.debug(f"No update needed for MMS ID '{mms_id}'.")
    return Record_confirmation(False, oclc_nums_from_record_str)


def init_argparse() -> argparse.ArgumentParser:
    """Initializes and returns ArgumentParser object."""

    parser = argparse.ArgumentParser(
        usage='%(prog)s [option] excel_file',
        description=f'For each row in the Excel file, add the corresponding ' \
            f'OCLC Number to the specified Alma record (indicated by the MMS ' \
            f'ID).',
    )
    parser.add_argument(
        '-v', '--version', action='version',
        version=f'{parser.prog} version 1.0.0'
    )
    parser.add_argument(
        'Excel_file',
        metavar='excel_file',
        type=str,
        help=f'the name and path of the input file, which must be in Excel ' \
            f'format (e.g. xlsx/filename.xlsx)'
    )
    return parser


# Initialize parser and parse command-line args
parser = init_argparse()
args = parser.parse_args()

# Convert excel file into pandas DataFrame
data = pd.read_excel(args.Excel_file, 'Sheet1', engine='openpyxl',
    dtype={'MMS ID': 'str', 'OCLC Number': 'str'}, keep_default_na=False)

# Loop over rows in DataFrame and update the corresponding Alma record
num_records_updated = 0
with open('xlsx/records_updated.csv', mode='a',
        newline='') as records_updated, \
    open('xlsx/records_with_no_update_needed.csv', mode='a',
        newline='') as records_with_no_update_needed, \
    open('xlsx/records_with_errors.csv', mode='a',
        newline='') as records_with_errors:

    records_updated_writer = writer(records_updated)
    records_with_no_update_needed_writer = writer(records_with_no_update_needed)
    records_with_errors_writer = writer(records_with_errors)

    for index, row in data.iterrows():
        error_occurred = True
        error_msg = None
        record = None
        try:
            record = update_alma_record(row['MMS ID'], row['OCLC Number'])
            if record.was_updated:
                num_records_updated += 1

                # add record to records_updated spreadsheet
                if records_updated.tell() == 0:
                    # write header row
                    records_updated_writer.writerow([ 'MMS ID',
                        'Original OCLC Number(s)', 'New OCLC Number' ])

                records_updated_writer.writerow([ row['MMS ID'],
                    record.orig_oclc_nums, row['OCLC Number'] ])
            else:
                # add record to records_with_no_update_needed spreadsheet
                if records_with_no_update_needed.tell() == 0:
                    # write header row
                    records_with_no_update_needed_writer.writerow([ 'MMS ID',
                        'OCLC Number' ])

                records_with_no_update_needed_writer.writerow([ row['MMS ID'],
                    row['OCLC Number'] ])

            error_occurred = False
        except AssertionError as assert_err:
            logger.exception(f"An assertion error occurred when processing " \
                f"MMS ID '{row['MMS ID']}': {assert_err}")
            error_msg = f"Assertion Error: {assert_err}"
        except HTTPError as http_err:
            logger.exception(f"An HTTP error occurred when processing MMS ID " \
                f"'{row['MMS ID']}': {http_err}")
            error_msg = f"HTTP Error: {http_err}"
        except Exception as err:
            logger.exception(f"An error occurred when processing MMS ID " \
                f"'{row['MMS ID']}': {err}")
            error_msg = err
        finally:
            if error_occurred:
                # add record to records_with_errors spreadsheet
                if records_with_errors.tell() == 0:
                    # write header row
                    records_with_errors_writer.writerow([ 'MMS ID',
                        "OCLC Number(s) from Alma Record's 035 $a",
                        'Current OCLC Number', 'Error' ])

                records_with_errors_writer.writerow([ row['MMS ID'],
                    record.orig_oclc_nums if record is not None
                    else '<record not checked>',
                    row['OCLC Number'], error_msg ])

print(f'\nEnd of script. {num_records_updated} of {len(data.index)} ' \
    f'records updated.')
