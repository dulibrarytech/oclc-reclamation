import argparse
import libraries.handle_file
import libraries.record
import logging
import logging.config
import os
import xml.etree.ElementTree as ET
from csv import writer
from datetime import datetime

logging.config.fileConfig(
    'logging.conf',
    defaults={'log_filename': 'logs/extract_record_identifiers.log'},
    disable_existing_loggers=False)
logger = logging.getLogger(__name__)


def init_argparse() -> argparse.ArgumentParser:
    """Initializes and returns ArgumentParser object."""

    parser = argparse.ArgumentParser(
        usage=('%(prog)s [option] directory_with_xml_files '
            '[alma_records_with_current_oclc_num]'),
        description=('For each XML file in the directory, extract the MMS ID '
            'and OCLC Number(s) from each Alma record and append them to '
            'the appropriate outputs/extract_record_identifiers/'
            'master_list_records CSV file.')
    )
    parser.add_argument(
        '-v', '--version', action='version',
        version=f'{parser.prog} version 1.0.0'
    )
    parser.add_argument(
        'directory_with_xml_files',
        type=str,
        help='the path to the directory containing the XML files to process'
    )
    parser.add_argument(
        'alma_records_with_current_oclc_num',
        nargs='?',
        type=str,
        help=('the name and path of the CSV file containing the MMS IDs of '
            'all Alma records with a current OCLC number (e.g. inputs/extract_'
            'record_identifiers/alma_records_with_current_oclc_num.csv)')
    )
    return parser


def main() -> None:
    """Extracts the MMS ID and OCLC Number(s) from each record of each XML file.

    For each XML file in the specified directory, the MMS ID and OCLC Number(s)
    from each Alma record are extracted and appended to the appropriate
    outputs/extract_record_identifiers/master_list_records CSV file:
    - If an error is encountered, then the record is added to:
      outputs/extract_record_identifiers/master_list_records_with_errors.csv
    - If the record's MMS ID appears in the optional
      alma_records_with_current_oclc_num input file, then the record is added
      to: outputs/extract_record_identifiers/
      master_list_records_with_current_oclc_num.csv
    - Otherwise, the record is added to: outputs/extract_record_identifiers/
      master_list_records_with_potentially_old_oclc_num.csv
    - If any of the above output files already exists in the directory, then it
      is appended to (not overwritten).
    """

    start_time = datetime.now()

    # Initialize parser and parse command-line args
    parser = init_argparse()
    args = parser.parse_args()

    directory_with_xml_files = args.directory_with_xml_files.rstrip('/')
    command_line_args_str = (f'command-line args:\n'
        f'directory_with_xml_files = {directory_with_xml_files}\n'
        f'alma_records_with_current_oclc_num = '
        f'{args.alma_records_with_current_oclc_num}')

    logger.info(f'Started {parser.prog} script with {command_line_args_str}')

    # Create sets
    mms_ids_already_processed = set()
    logger.debug(f'{mms_ids_already_processed=}')
    logger.debug(f'{type(mms_ids_already_processed)=}\n')

    # Populate alma_records_with_current_oclc_num set from input file.
    # If there is no input file, then this set will remain empty.
    alma_records_with_current_oclc_num = set()
    libraries.handle_file.csv_column_to_set(
        args.alma_records_with_current_oclc_num,
        alma_records_with_current_oclc_num,
        0,
        True)
    # logger.debug(f'{alma_records_with_current_oclc_num=}')
    logger.debug(f'{len(alma_records_with_current_oclc_num)=}')
    logger.debug(f'{type(alma_records_with_current_oclc_num)=}\n')

    with open('outputs/extract_record_identifiers/master_list_records_with_'
            'current_oclc_num.csv', mode='a',
            newline='') as records_with_current_oclc_num, \
        open('outputs/extract_record_identifiers/master_list_records_with_'
            'potentially_old_oclc_num.csv', mode='a',
            newline='') as records_with_potentially_old_oclc_num, \
        open('outputs/extract_record_identifiers/master_list_records_with_'
            'errors.csv', mode='a',
            newline='') as records_with_errors:

        records_with_current_oclc_num_writer = \
            writer(records_with_current_oclc_num)
        records_with_potentially_old_oclc_num_writer = \
            writer(records_with_potentially_old_oclc_num)
        records_with_errors_writer = writer(records_with_errors)

        all_oclc_nums_col_heading = (f"All OCLC Numbers from Alma Record's "
            f"035 $a [for this column and the previous one, "
            f"{libraries.record.subfield_a_disclaimer}]")

        # Check every XML file in directory
        logger.debug(f'Started checking directory: {directory_with_xml_files}'
            f'\n')

        for file in os.listdir(directory_with_xml_files):
            if not file.endswith('.xml'):
                logger.warning(f'Not an XML file: {file}\n')
                continue

            logger.debug(f'Started processing file: {file}\n')

            # Get root element of XML file
            root = ET.parse(f'{directory_with_xml_files}/{file}').getroot()

            # Iterate over each record element
            for record_element in root.findall('record'):
                # Extract MMS ID from 001 field
                mms_id = record_element.find('./controlfield[@tag="001"]').text

                # Check if MMS ID is a member of mms_ids_already_processed set
                if mms_id in mms_ids_already_processed:
                    logger.debug(f'{mms_id} has already been processed\n')
                    continue

                logger.debug(f'Started processing MMS ID {mms_id}')

                # Add MMS ID to mms_ids_already_processed set
                mms_ids_already_processed.add(mms_id)

                # Iterate over each 035 $a field and add OCLC numbers to list
                # and set
                all_oclc_nums_from_record = list()
                unique_oclc_nums_from_record = set()
                found_error_in_record = False
                error_msg = None

                for field_035_element_index, field_035_element in enumerate(
                        record_element.findall('./datafield[@tag="035"]')):
                    # Extract subfield a (which would contain the OCLC number
                    # if present)
                    subfield_a_data = \
                        libraries.record.get_subfield_a_with_oclc_num(
                            field_035_element,
                            field_035_element_index)

                    # Add or append to error message
                    if subfield_a_data.error_msg is not None:
                        if error_msg is None:
                            error_msg = subfield_a_data.error_msg
                        else:
                            error_msg += '. ' + subfield_a_data.error_msg

                    if subfield_a_data.string_with_oclc_num is None:
                        # This 035 field either has no subfield $a or its first
                        # subfield $a does not contain an OCLC number. So skip
                        # it.
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

                    all_oclc_nums_from_record.append(
                        subfield_a_without_oclc_org_code_prefix)

                    unique_oclc_nums_from_record.add(extracted_oclc_num)

                unique_oclc_nums_from_record_len = \
                    len(unique_oclc_nums_from_record)
                unique_oclc_nums_from_record_str = None
                all_oclc_nums_from_record_str = (
                    '<none>' if len(all_oclc_nums_from_record) == 0
                    else ', '.join(all_oclc_nums_from_record))

                logger.debug(f'{unique_oclc_nums_from_record=}')
                logger.debug(f'{all_oclc_nums_from_record=}')

                if unique_oclc_nums_from_record_len == 0:
                    unique_oclc_nums_from_record_str = '<none>'
                    logger.debug(f'{mms_id} has no OCLC number in an 035 '
                        f'$a field')
                    found_error_in_record = True
                elif unique_oclc_nums_from_record_len == 1:
                    unique_oclc_nums_from_record_str = \
                        next(iter(unique_oclc_nums_from_record))
                    if found_error_in_record:
                        logger.debug(f'{mms_id} has at least one invalid '
                            f'OCLC number: '
                            f'{unique_oclc_nums_from_record_str}')
                else:
                    # unique_oclc_nums_from_record_len > 1
                    unique_oclc_nums_from_record_str = \
                        ', '.join(unique_oclc_nums_from_record)
                    logger.debug(f'{mms_id} has multiple OCLC numbers: '
                        f'{unique_oclc_nums_from_record_str}')
                    found_error_in_record = True

                if found_error_in_record:
                    # Add record to records_with_errors spreadsheet
                    if records_with_errors.tell() == 0:
                        # Write header row
                        records_with_errors_writer.writerow([
                            'MMS ID',
                            "Unique OCLC Number(s) from Alma Record's 035 $a",
                            all_oclc_nums_col_heading,
                            'Error'
                        ])

                    records_with_errors_writer.writerow([
                        mms_id,
                        unique_oclc_nums_from_record_str,
                        all_oclc_nums_from_record_str,
                        error_msg
                    ])
                elif mms_id in alma_records_with_current_oclc_num:
                    logger.debug(f'{mms_id} has current OCLC number')

                    # Add record to records_with_current_oclc_num spreadsheet
                    if records_with_current_oclc_num.tell() == 0:
                        # Write header row
                        records_with_current_oclc_num_writer.writerow([
                            'MMS ID',
                            ("Current OCLC Number (Unique OCLC Number from "
                                "Alma Record's 035 $a)"),
                            all_oclc_nums_col_heading,
                            'Warning'
                        ])

                    records_with_current_oclc_num_writer.writerow([
                        mms_id,
                        unique_oclc_nums_from_record_str,
                        all_oclc_nums_from_record_str,
                        error_msg
                    ])
                else:
                    logger.debug(f'{mms_id} has a potentially-old OCLC number')

                    # Add record to records_with_current_oclc_num spreadsheet
                    if records_with_potentially_old_oclc_num.tell() == 0:
                        # Write header row
                        records_with_potentially_old_oclc_num_writer.writerow([
                            'MMS ID',
                            "Unique OCLC Number from Alma Record's 035 $a",
                            all_oclc_nums_col_heading,
                            'Warning'
                        ])

                    records_with_potentially_old_oclc_num_writer.writerow([
                        mms_id,
                        unique_oclc_nums_from_record_str,
                        all_oclc_nums_from_record_str,
                        error_msg
                    ])

                logger.debug(f'Finished processing MMS ID {mms_id}\n')

            logger.debug(f'Finished processing file: {file}\n')

        logger.debug(f'Finished checking directory: {directory_with_xml_files}'
            f'\n')

    # logger.debug(f'{mms_ids_already_processed=}\n')
    logger.debug(f'{len(mms_ids_already_processed)=}\n')

    logger.info(f'Finished {parser.prog} script with {command_line_args_str}\n')

    print(f'End of script. Completed in: {datetime.now() - start_time} ' \
        f'(hours:minutes:seconds.microseconds)\n')


if __name__ == "__main__":
    main()
