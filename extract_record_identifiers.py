import argparse
import logging
import logging.config
import os
import re
import xml.etree.ElementTree as ET
from csv import reader, writer
from datetime import datetime

logging.config.fileConfig('logging.conf', disable_existing_loggers=False)
logger = logging.getLogger(__name__)


def init_argparse() -> argparse.ArgumentParser:
    """Initializes and returns ArgumentParser object."""

    parser = argparse.ArgumentParser(
        usage='%(prog)s [option] directory_with_xml_files ' \
            '[alma_records_with_current_oclc_num]',
        description=f'For each XML file in the directory, extract the MMS ID ' \
            f'and OCLC Number(s) from each Alma record and append them to ' \
            f'the appropriate master_list_records CSV file.',
    )
    parser.add_argument(
        '-v', '--version', action='version',
        version=f'{parser.prog} version 1.0.0'
    )
    parser.add_argument(
        'Directory_with_xml_files',
        metavar='directory_with_xml_files',
        type=str,
        help=f'the path to the directory containing the XML files to process'
    )
    parser.add_argument(
        'Alma_records_with_current_oclc_num',
        metavar='alma_records_with_current_oclc_num',
        nargs='?',
        const=None,
        type=str,
        help=f'the name and path of the CSV file containing the MMS IDs of ' \
            f'all Alma records with a current OCLC number (e.g. ' \
            f'csv/alma_records_with_current_oclc_num.csv)'
    )
    return parser


def main() -> None:
    """Extracts the MMS IDs and OCLC Numbers from each record in the XML files.

    For each XML file in the specified directory, the MMS ID and OCLC Number(s)
    from each Alma record are extracted and appended to the appropriate
    master_list_records CSV file.
    """
    start_time = datetime.now()

    # Initialize parser and parse command-line args
    parser = init_argparse()
    args = parser.parse_args()

    # Create sets
    mms_ids_already_processed = set()
    logger.debug(f'\n{mms_ids_already_processed=}')
    logger.debug(f'{type(mms_ids_already_processed)=}')

    alma_records_with_current_oclc_num = set()
    if args.Alma_records_with_current_oclc_num is not None:
        with open(args.Alma_records_with_current_oclc_num, mode='r',
            newline='') as file:
            file_reader = reader(file)
            for mms_id in file_reader:
                alma_records_with_current_oclc_num.add(mms_id[0])

    logger.debug(f'\n{alma_records_with_current_oclc_num=}')
    logger.debug(f'{type(alma_records_with_current_oclc_num)=}\n')

    oclc_org_code_prefix = '(OCoLC)'
    oclc_org_code_prefix_len = len(oclc_org_code_prefix)

    with open('csv/master_list_records_with_current_oclc_num.csv', mode='a',
            newline='') as records_with_current_oclc_num, \
        open('csv/master_list_records_with_potentially_old_oclc_num.csv',
            mode='a', newline='') as records_with_potentially_old_oclc_num, \
        open('csv/master_list_records_with_errors.csv', mode='a',
            newline='') as records_with_errors:

        records_with_current_oclc_num_writer = \
            writer(records_with_current_oclc_num)
        records_with_potentially_old_oclc_num_writer = \
            writer(records_with_potentially_old_oclc_num)
        records_with_errors_writer = writer(records_with_errors)

        # Check every XML file in directory
        for file in os.listdir(args.Directory_with_xml_files):
            if not file.endswith('.xml'):
                logger.debug(f'\n{file} is not an XML file')
                continue

            logger.debug(f'\n{file} is an XML file')
            logger.debug(f'Parsing {file} ...')

            # Get root element of XML file
            root = ET.parse(f'{args.Directory_with_xml_files}/{file}').getroot()

            # Iterate over each record element
            for record_element in root.findall('record'):
                # Extract MMS ID from 001 field
                mms_id = record_element.find('./controlfield[@tag="001"]').text
                logger.debug(f'\n{mms_id=}')

                # Check if MMS ID is a member of mms_ids_already_processed set
                if mms_id in mms_ids_already_processed:
                    logger.debug(f'{mms_id} has already been processed')
                    continue

                logger.debug(f'Processing {mms_id}...')

                # Add MMS ID to mms_ids_already_processed set
                mms_ids_already_processed.add(mms_id)

                # Iterate over each 035 $a field and add OCLC numbers to list
                # and set
                all_oclc_nums_from_record = list()
                unique_oclc_nums_from_record = set()

                for i, field_035_element in enumerate(
                    record_element.findall('./datafield[@tag="035"]')):
                    # Extract subfield a (which would contain the OCLC number
                    # if present)
                    subfield_a_element = \
                        field_035_element.find('./subfield[@code="a"]')
                    if subfield_a_element is None:
                        continue

                    subfield_a = subfield_a_element.text
                    logger.debug(f'035 field #{i + 1}, subfield a: ' \
                        f'{subfield_a}')

                    # Skip this 035 field if it's not an OCLC number
                    if not subfield_a.startswith(oclc_org_code_prefix):
                        continue

                    # Extract the OCLC number itself
                    oclc_num_without_org_code_prefix = \
                        subfield_a[oclc_org_code_prefix_len:].strip()

                    match_on_first_digit = re.search(r'\d',
                        oclc_num_without_org_code_prefix)

                    if oclc_num_without_org_code_prefix == '':
                        oclc_num_without_org_code_prefix = \
                            f'<nothing after {oclc_org_code_prefix}>'

                    extracted_oclc_num_from_record = \
                        oclc_num_without_org_code_prefix

                    if match_on_first_digit is None:
                        logger.debug(f'This OCLC number has no digits: ' \
                            f'{subfield_a}')
                    else:
                        extracted_oclc_num_from_record = \
                            oclc_num_without_org_code_prefix[
                                match_on_first_digit.start():].strip()

                    logger.debug(f'035 field #{i + 1}, extracted OCLC ' \
                        f'number: {extracted_oclc_num_from_record}')

                    all_oclc_nums_from_record.append(
                        oclc_num_without_org_code_prefix)

                    unique_oclc_nums_from_record.add(
                        extracted_oclc_num_from_record)

                logger.debug(f'{unique_oclc_nums_from_record=}')
                logger.debug(f'{all_oclc_nums_from_record=}')

                error_found = False
                unique_oclc_nums_from_record_len = \
                    len(unique_oclc_nums_from_record)
                unique_oclc_nums_from_record_str = None

                if unique_oclc_nums_from_record_len == 0:
                    unique_oclc_nums_from_record_str = '<none>'
                    logger.debug(f'{mms_id} has no OCLC numbers in 035 $a')
                    error_found = True
                elif unique_oclc_nums_from_record_len == 1:
                    unique_oclc_nums_from_record_str = \
                        next(iter(unique_oclc_nums_from_record))
                    if not unique_oclc_nums_from_record_str.isdigit():
                        logger.debug(f'{mms_id} has an OCLC number with at ' \
                            f'least one non-digit character: ' \
                            f'{unique_oclc_nums_from_record_str}')
                        error_found = True
                else:
                    # unique_oclc_nums_from_record_len > 1
                    unique_oclc_nums_from_record_str = \
                        ', '.join(unique_oclc_nums_from_record)
                    logger.debug(f'{mms_id} has multiple OCLC numbers: ' \
                        f'{unique_oclc_nums_from_record_str}')
                    error_found = True

                if error_found:
                    # Add record to records_with_errors spreadsheet
                    if records_with_errors.tell() == 0:
                        # Write header row
                        records_with_errors_writer.writerow([ 'MMS ID',
                            "Unique OCLC Number(s) from Alma Record's 035 $a",
                            "All OCLC Numbers from Alma Record's 035 $a" ])

                    records_with_errors_writer.writerow([ mms_id,
                        unique_oclc_nums_from_record_str,
                        '<none>' if len(all_oclc_nums_from_record) == 0
                        else ', '.join(all_oclc_nums_from_record) ])

                    # Go to next record
                    continue

                # Check if MMS ID is a member of
                # alma_records_with_current_oclc_num set
                if mms_id in alma_records_with_current_oclc_num:
                    logger.debug(f'{mms_id} has current OCLC number')

                    # Add record to records_with_current_oclc_num spreadsheet
                    if records_with_current_oclc_num.tell() == 0:
                        # Write header row
                        records_with_current_oclc_num_writer.writerow([
                            'MMS ID', 'Current OCLC Number' ])

                    records_with_current_oclc_num_writer.writerow([ mms_id,
                        unique_oclc_nums_from_record_str ])
                else:
                    logger.debug(f'{mms_id} has a potentially old OCLC number')

                    # Add record to records_with_current_oclc_num spreadsheet
                    if records_with_potentially_old_oclc_num.tell() == 0:
                        # Write header row
                        records_with_potentially_old_oclc_num_writer.writerow([
                            'MMS ID',
                            "Unique OCLC Number from Alma Record's 035 $a",
                            "All OCLC Numbers from Alma Record's 035 $a" ])

                    records_with_potentially_old_oclc_num_writer.writerow([
                        mms_id, unique_oclc_nums_from_record_str,
                        '<none>' if len(all_oclc_nums_from_record) == 0
                        else ', '.join(all_oclc_nums_from_record) ])

    logger.debug(f'\n{mms_ids_already_processed=}')

    print(f'\nEnd of script. Completed in: {datetime.now() - start_time} ' \
        f'(hours:minutes:seconds.microseconds)')


if __name__ == "__main__":
    main()
