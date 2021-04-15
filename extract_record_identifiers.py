import argparse
import logging
import logging.config
import os
import xml.etree.ElementTree as ET
from csv import reader, writer

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

    # Initialize parser and parse command-line args
    parser = init_argparse()
    args = parser.parse_args()

    # Create sets
    mms_ids_already_processed = set()
    print(f'\n{mms_ids_already_processed=}')
    print(f'{type(mms_ids_already_processed)=}')

    alma_records_with_current_oclc_num = set()
    if args.Alma_records_with_current_oclc_num is not None:
        with open(args.Alma_records_with_current_oclc_num, mode='r',
            newline='') as file:
            file_reader = reader(file)
            for mms_id in file_reader:
                alma_records_with_current_oclc_num.add(mms_id[0])

    print(f'\n{alma_records_with_current_oclc_num=}')
    print(f'{type(alma_records_with_current_oclc_num)=}\n')

    # Check every XML file in directory
    for file in os.listdir(args.Directory_with_xml_files):
        if not file.endswith('.xml'):
            print(f'{file} is not an XML file')
            continue

        print(f'{file} is an XML file')

        # Get XML file as ET.Element

        # for record in ET.Element:
            # Extract MMS ID from 001 field

            # Check if MMS ID is a member of mms_ids_already_processed set
            # If so, continue (i.e. skip to next record)

            # Add MMS ID to mms_ids_already_processed set

            # Extract all OCLC numbers from 035 $a fields and add to list

            # If list length > 1, append MMS ID and OCLC numbers to
            # master_list_records_with_errors CSV file (an error message within the
            # CSV file probably isn't necessary for this case).

            # Check if MMS ID is a member of alma_records_with_current_oclc_num set
            # - If so, append MMS ID and OCLC number to
            #   master_list_records_with_current_oclc_num CSV file
            # - If not, append MMS ID and OCLC number to
            #   master_list_records_with_potentially_old_oclc_num CSV file


if __name__ == "__main__":
    main()
