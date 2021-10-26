import argparse
import libraries.api
import libraries.record
import libraries.xml
import logging
import logging.config
import os
import pandas as pd
import requests
import xml.etree.ElementTree as ET
from csv import writer
from dotenv import load_dotenv
from requests.exceptions import HTTPError
from typing import NamedTuple, Optional

load_dotenv()

logging.config.fileConfig('logging.conf', disable_existing_loggers=False)
logger = logging.getLogger(__name__)

alma_bibs_api_url = os.environ["ALMA_BIBS_API_URL"]
headers = {'Authorization': f'apikey {os.environ["ALMA_API_KEY"]}'}
params = {'view': 'full'}


class Record_confirmation(NamedTuple):
    """Details about a specific call to the update_alma_record function.

    Instances should adhere to the following rule:
    - If the was_updated field is True, then the error_msg field should be None.

    Fields
    ------
    was_updated: bool
        True if the update_alma_record function call resulted in the record
        actually being updated; otherwise, False
    orig_oclc_nums: Optional[str]
        A comma-separated listing of the original OCLC Number(s) from the Alma
        record's 035 $a field(s), provided that no errors caused the process to
        abort; otherwise, None
    error_msg: Optional[str]
        Message explaining the error(s) and/or warning(s) encountered by the
        update_alma_record function call, if applicable; otherwise, None
    """
    was_updated: bool
    orig_oclc_nums: Optional[str]
    error_msg: Optional[str]


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

    response = requests.get(
        f'{alma_bibs_api_url}{mms_id}',
        params=params,
        headers=headers,
        timeout=45)
    libraries.api.log_response_and_raise_for_status(response)

    xml_as_pretty_printed_bytes_obj = \
        libraries.xml.prettify_and_log_xml(response, 'Original record')

    # Create XML file
    with open(f'xml/{mms_id}_original.xml', 'wb') as file:
        file.write(xml_as_pretty_printed_bytes_obj)

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
        following fields: was_updated, orig_oclc_nums, error_msg
    """

    logger.debug(f"Attempting to update MMS ID '{mms_id}'...")

    # Make sure that mms_id and oclc_num are valid
    mms_id = libraries.record.get_valid_record_identifier(mms_id, 'MMS ID')
    oclc_num = libraries.record.get_valid_record_identifier(oclc_num,
        'OCLC number')

    # Remove leading zeros and create full OCLC number string
    oclc_num = libraries.record.remove_leading_zeros(oclc_num)
    full_oclc_num = f'{libraries.record.oclc_org_code_prefix}{oclc_num}'

    logger.debug(f'Full OCLC number: {full_oclc_num}')

    # Access XML elements of Alma record
    root = get_alma_record(mms_id)
    record_element = root.find('./record')

    need_to_update_record = False
    oclc_nums_from_record = list()
    oclc_nums_for_019_field = set()
    found_035_field_with_current_oclc_num = False
    record_contains_potentially_valid_oclc_num_with_invalid_oclc_prefix = False
    error_msg = None
    found_error_in_record = False

    # Iterate over each 035 field
    for field_035_element_index, field_035_element in enumerate(
            record_element.findall('./datafield[@tag="035"]')):
        # Extract subfield a (which would contain the OCLC number if present)
        subfield_a_data = libraries.record.get_subfield_a_with_oclc_num(
            field_035_element,
            field_035_element_index)

        # Add or append to error message
        if subfield_a_data.error_msg is not None:
            if error_msg is None:
                error_msg = subfield_a_data.error_msg
            else:
                error_msg += '. ' + subfield_a_data.error_msg

        if subfield_a_data.string_with_oclc_num is None:
            # This 035 field either has no subfield $a or its first subfield $a
            # does not contain an OCLC number. So skip it.
            continue

        (subfield_a_without_oclc_org_code_prefix,
                extracted_oclc_num,
                found_valid_oclc_prefix,
                found_valid_oclc_num,
                found_error_in_record) = \
            libraries.record.extract_oclc_num_from_subfield_a(
                subfield_a_data.string_with_oclc_num,
                field_035_element_index,
                found_error_in_record)

        oclc_nums_from_record.append(subfield_a_without_oclc_org_code_prefix)

        # Check for potentially-valid OCLC number with invalid prefix
        found_potentially_valid_oclc_num_with_invalid_oclc_prefix = \
            found_valid_oclc_num and not found_valid_oclc_prefix

        if found_potentially_valid_oclc_num_with_invalid_oclc_prefix:
            invalid_prefix_msg = (f'035 field #{field_035_element_index + 1} '
                f'contains an OCLC number with an invalid prefix: '
                f'{extracted_oclc_num}. '
                f'{libraries.record.valid_oclc_number_prefixes_str}')
            if error_msg is None:
                error_msg = invalid_prefix_msg
            else:
                error_msg += '. ' + invalid_prefix_msg

        record_contains_potentially_valid_oclc_num_with_invalid_oclc_prefix = \
            (record_contains_potentially_valid_oclc_num_with_invalid_oclc_prefix
             or found_potentially_valid_oclc_num_with_invalid_oclc_prefix)

        if not record_contains_potentially_valid_oclc_num_with_invalid_oclc_prefix:
            # Compare the extracted OCLC number to the current OCLC number
            extracted_oclc_num_matches_current_oclc_num = \
                extracted_oclc_num == oclc_num
            logger.debug(f'Does the extracted OCLC number '
                f'({extracted_oclc_num}) match the current OCLC number '
                f'({oclc_num})? {extracted_oclc_num_matches_current_oclc_num}')

            if (not extracted_oclc_num_matches_current_oclc_num
                    or found_035_field_with_current_oclc_num):
                # This 035 field either (1) contains an old, empty or invalid
                # OCLC number or (2) is a duplicate of another 035 field with
                # the current OCLC number. In either case, remove this 035
                # field.
                record_element.remove(field_035_element)
                logger.debug(f'Removed 035 field #'
                    f'{field_035_element_index + 1}, whose (first) subfield a '
                    f'is: {subfield_a_data.string_with_oclc_num}')

                if (not extracted_oclc_num_matches_current_oclc_num
                        and len(extracted_oclc_num) > 0
                        and found_valid_oclc_num):
                    oclc_nums_for_019_field.add(extracted_oclc_num)

                need_to_update_record = True
            else:
                found_035_field_with_current_oclc_num = True

    logger.debug(f'{oclc_nums_for_019_field=}')
    logger.debug(f'{len(oclc_nums_for_019_field)=}')

    oclc_nums_from_record_list_length = len(oclc_nums_from_record)
    oclc_nums_from_record_str = None
    if oclc_nums_from_record_list_length > 0:
        oclc_nums_from_record_str = ', '.join(oclc_nums_from_record)

    logger.debug(f'{oclc_nums_from_record=}')
    logger.debug(f'{oclc_nums_from_record_list_length=}')
    logger.debug(f'{oclc_nums_from_record_str=}')

    # Don't update the record if it contains a potentially-valid OCLC number
    # with an invalid prefix.
    if record_contains_potentially_valid_oclc_num_with_invalid_oclc_prefix:
        logger.debug(f"Did not update MMS ID '{mms_id}' because it contains at "
            f"least one potentially-valid OCLC number with an invalid prefix.")

        return Record_confirmation(False, oclc_nums_from_record_str, error_msg)

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
            logger.debug(f'First 019 element after adding {old_oclc_num}:\n'
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

        put_response = requests.put(
            f'{alma_bibs_api_url}{mms_id}',
            headers=headers,
            data=payload,
            timeout=45)
        libraries.api.log_response_and_raise_for_status(put_response)

        xml_as_pretty_printed_bytes_obj = libraries.xml.prettify_and_log_xml(
            put_response, 'Modified record')

        # Create XML file
        with open(f'xml/{mms_id}_modified.xml', 'wb') as file:
            file.write(xml_as_pretty_printed_bytes_obj)

        logger.debug(f"MMS ID '{mms_id}' has been updated.")
        return Record_confirmation(True, oclc_nums_from_record_str, None)

    logger.debug(f"No update needed for MMS ID '{mms_id}'.")
    return Record_confirmation(False, oclc_nums_from_record_str, None)


def init_argparse() -> argparse.ArgumentParser:
    """Initializes and returns ArgumentParser object."""

    parser = argparse.ArgumentParser(
        usage='%(prog)s [option] input_file',
        description=(f'For each row in the input file, add the corresponding '
            f'OCLC Number to the specified Alma record (indicated by the MMS '
            f'ID).')
    )
    parser.add_argument(
        '-v', '--version', action='version',
        version=f'{parser.prog} version 1.0.0'
    )
    parser.add_argument(
        'Input_file',
        metavar='input_file',
        type=str,
        help=(f'the name and path of the input file, which must be in either '
            f'CSV or Excel format (e.g. xlsx/filename.xlsx)')
    )
    return parser


def main() -> None:
    """Updates Alma records to have the corresponding OCLC number.

    For each row in the input file, the corresponding OCLC number is added to
    the specified Alma record (indicated by the MMS ID), unless the Alma record
    already contains that OCLC number. If the Alma record contains non-matching
    OCLC numbers in an 035 field (in the subfield $a), those OCLC numbers are
    moved to the 019 field.
    """

    # Initialize parser and parse command-line args
    parser = init_argparse()
    args = parser.parse_args()

    # Convert input file into pandas DataFrame
    data = None
    if args.Input_file.endswith('.csv'):
        data = pd.read_csv(args.Input_file,
            dtype={'MMS ID': 'str', 'OCLC Number': 'str'},
            keep_default_na=False)
    elif args.Input_file.endswith('.xlsx'):
        data = pd.read_excel(args.Input_file, 'Sheet1', engine='openpyxl',
            dtype={'MMS ID': 'str', 'OCLC Number': 'str'},
            keep_default_na=False)
    elif args.Input_file.endswith('.xls'):
        data = pd.read_excel(args.Input_file, 'Sheet1', engine='xlrd',
            dtype={'MMS ID': 'str', 'OCLC Number': 'str'},
            keep_default_na=False)
    else:
        logger.exception(f'Invalid format for input file ({args.Input_file}). '
            f'Input file must be one of the following file formats (as '
            f'indicated by its file extension): CSV file (.csv) or Excel file '
            f'(.xlsx or .xls)')
        return

    # Loop over rows in DataFrame and update the corresponding Alma record
    num_records_updated = 0
    with open('xlsx/records_updated.csv', mode='a',
            newline='') as records_updated, \
        open('xlsx/records_with_no_update_needed.csv', mode='a',
            newline='') as records_with_no_update_needed, \
        open('xlsx/records_with_errors.csv', mode='a',
            newline='') as records_with_errors:

        records_updated_writer = writer(records_updated)
        records_with_no_update_needed_writer = \
            writer(records_with_no_update_needed)
        records_with_errors_writer = writer(records_with_errors)

        for index, row in data.iterrows():
            error_occurred = True
            error_msg = None
            record = None
            try:
                # Note: The update_alma_record function returns a
                # Record_confirmation NamedTuple which should adhere to the
                # following rule:
                # - If the was_updated field is True, then the error_msg field
                # will be None.
                record = update_alma_record(row['MMS ID'], row['OCLC Number'])
                if record.error_msg is None:
                    error_occurred = False
                else:
                    error_msg = record.error_msg

                if record.was_updated:
                    num_records_updated += 1

                    # Add record to records_updated spreadsheet
                    if records_updated.tell() == 0:
                        # Write header row
                        records_updated_writer.writerow([
                            'MMS ID',
                            (f'Original OCLC Number(s) '
                                f'[{libraries.record.subfield_a_disclaimer}]'),
                            'New OCLC Number'
                        ])

                    records_updated_writer.writerow([
                        row['MMS ID'],
                        record.orig_oclc_nums,
                        row['OCLC Number']
                    ])
                elif record.error_msg is None:
                    # Add record to records_with_no_update_needed spreadsheet
                    if records_with_no_update_needed.tell() == 0:
                        # Write header row
                        records_with_no_update_needed_writer.writerow([
                            'MMS ID',
                            'OCLC Number'
                        ])

                    records_with_no_update_needed_writer.writerow([
                        row['MMS ID'],
                        row['OCLC Number']
                    ])
            except AssertionError as assert_err:
                logger.exception(f"An assertion error occurred when "
                    f"processing MMS ID '{row['MMS ID']}' (at row {index + 2}"
                    f" of input file): {assert_err}")
                error_msg = f"Assertion Error: {assert_err}"
            except HTTPError as http_err:
                logger.exception(f"An HTTP error occurred when processing "
                    f"MMS ID '{row['MMS ID']}' (at row {index + 2} of input "
                    f"file): {http_err}")
                error_msg = f"HTTP Error: {http_err}"
            except Exception as err:
                logger.exception(f"An error occurred when processing MMS ID "
                    f"'{row['MMS ID']}' (at row {index + 2} of input file): "
                    f"{err}")
                error_msg = err
            finally:
                if error_occurred:
                    # Add record to records_with_errors spreadsheet
                    if records_with_errors.tell() == 0:
                        # Write header row
                        records_with_errors_writer.writerow([
                            'MMS ID',
                            (f'OCLC Number(s) from Alma Record '
                                f'[{libraries.record.subfield_a_disclaimer}]'),
                            'Current OCLC Number',
                            'Error'
                        ])

                    records_with_errors_writer.writerow([
                        row['MMS ID'],
                        record.orig_oclc_nums if record is not None
                            and record.orig_oclc_nums is not None
                            else '<record not fully checked>',
                        row['OCLC Number'],
                        error_msg
                    ])

    print(f'\nEnd of script. {num_records_updated} of {len(data.index)} '
        f'records updated.')


if __name__ == "__main__":
    main()
