import argparse
import logging
import logging.config
from csv import reader, writer
from datetime import datetime

logging.config.fileConfig('logging.conf', disable_existing_loggers=False)
logger = logging.getLogger(__name__)


def init_argparse() -> argparse.ArgumentParser:
    """Initializes and returns ArgumentParser object."""

    parser = argparse.ArgumentParser(
        usage=('%(prog)s [option] alma_records worldcat_records'),
        description=('Compare the alma_records and worldcat_records CSV files. '
            'Add the OCLC numbers found in both files to '
            'records_with_no_action_needed.csv. Add the OCLC numbers found in '
            'alma_records but not worldcat_records to '
            'records_to_set_in_worldcat.csv. Add the OCLC numbers found in '
            'worldcat_records but not alma_records to '
            'records_to_unset_in_worldcat.csv.'),
    )
    parser.add_argument(
        '-v', '--version', action='version',
        version=f'{parser.prog} version 1.0.0'
    )
    parser.add_argument(
        'Alma_records',
        metavar='alma_records',
        type=str,
        help=('the name and path of the CSV file containing the Alma records '
            'whose holdings **should be set** in WorldCat (e.g. '
            'csv/alma_master_list.csv); this file should consist of a single '
            'column named "OCLC Number"')
    )
    parser.add_argument(
        'Worldcat_records',
        metavar='worldcat_records',
        type=str,
        help=('the name and path of the CSV file containing the OCLC number '
            'of all records whose holdings **are currently set** in WorldCat '
            'for your institution (e.g. csv/worldcat_holdings_list.csv); this '
            'file should consist of a single column named "OCLC Number"')
    )
    return parser


def main() -> None:
    """Compares the Alma records to the current WorldCat holdings.

    Outputs the following files:
    - records_with_no_action_needed.csv
        The OCLC numbers found in both input files
    - records_to_set_in_worldcat.csv
        The OCLC numbers found in the alma_records input file but not the
        worldcat_records input file
    - records_to_unset_in_worldcat.csv
        The OCLC numbers found in the worldcat_records input file but not the
        alma_records input file
    """
    start_time = datetime.now()

    # Initialize parser and parse command-line args
    parser = init_argparse()
    args = parser.parse_args()

    # Create sets from each input file
    # If you don't use the reader function from the csv module, don't import it

    # Perform set comparisons and add results to appropriate output file
    with open('csv/records_with_no_action_needed.csv', mode='a',
            newline='') as records_in_both_sets, \
        open('csv/records_to_set_in_worldcat.csv',
            mode='a', newline='') as records_in_alma_not_worldcat, \
        open('csv/records_to_unset_in_worldcat.csv', mode='a',
            newline='') as records_in_worldcat_not_alma:

        records_in_both_sets_writer = writer(records_in_both_sets)
        records_in_alma_not_worldcat_writer = \
            writer(records_in_alma_not_worldcat)
        records_in_worldcat_not_alma_writer = \
            writer(records_in_worldcat_not_alma)

        # Perform intersection of sets

        # Perform set difference (alma_records - worldcat_records)

        # Perform set difference (worldcat_records - alma_records)

    print(f'End of script. Completed in: {datetime.now() - start_time} ' \
        f'(hours:minutes:seconds.microseconds)')


if __name__ == "__main__":
    main()
