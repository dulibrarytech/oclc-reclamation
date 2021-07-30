import argparse
import libraries.handle_file
import logging
import logging.config
import os
from csv import writer
from datetime import datetime

logging.config.fileConfig('logging.conf', disable_existing_loggers=False)
logger = logging.getLogger(__name__)


def init_argparse() -> argparse.ArgumentParser:
    """Initializes and returns ArgumentParser object."""

    parser = argparse.ArgumentParser(
        usage='%(prog)s [option] alma_records worldcat_records',
        description='Compare the alma_records and worldcat_records CSV files.',
    )
    parser.add_argument(
        '-v', '--version', action='version',
        version=f'{parser.prog} version 1.0.0'
    )
    parser.add_argument(
        'Alma_records',
        metavar='alma_records',
        type=str,
        help=('the name and path of the CSV file containing the records in '
            'Alma whose holdings **should be set** in WorldCat (e.g. '
            'csv/alma_master_list.csv); this file should consist of a single '
            'column with one OCLC number per row')
    )
    parser.add_argument(
        'Directory_with_worldcat_records',
        metavar='directory_with_worldcat_records',
        type=str,
        help=('the path to the directory of files containing the records whose '
            'holdings **are currently set** in WorldCat for your institution; '
            'each file should consist of a single column with one OCLC number '
            'per row')
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
    alma_records_set = set()
    libraries.handle_file.csv_column_to_set(args.Alma_records,
        alma_records_set,
        0,
        False)
    # logger.debug(f'{alma_records_set=}')
    logger.debug(f'{len(alma_records_set)=}\n')

    worldcat_records_set = set()

    # Check every file in directory
    for file in os.listdir(args.Directory_with_worldcat_records):
        if not file.endswith('.txt'):
            logger.debug(f'Not a text (.txt) file: {file}\n')
            continue

        logger.debug(f'Started processing file: {file}\n')

        libraries.handle_file.csv_column_to_set(
            f'{args.Directory_with_worldcat_records}/{file}',
            worldcat_records_set,
            0,
            False)

        logger.debug(f'Finished processing file: {file}\n')

    # logger.debug(f'{worldcat_records_set=}')
    logger.debug(f'{len(worldcat_records_set)=}\n')

    # Perform set comparisons and add results to appropriate output file
    with open('csv/records_with_no_action_needed.csv', mode='w',
            newline='') as records_in_both_sets, \
        open('csv/records_to_set_in_worldcat.csv', mode='w',
            newline='') as records_in_alma_not_worldcat, \
        open('csv/records_to_unset_in_worldcat.csv', mode='w',
            newline='') as records_in_worldcat_not_alma:

        records_in_both_sets_writer = writer(records_in_both_sets)
        records_in_alma_not_worldcat_writer = \
            writer(records_in_alma_not_worldcat)
        records_in_worldcat_not_alma_writer = \
            writer(records_in_worldcat_not_alma)

        # Perform intersection of sets
        alma_worldcat_intersection = alma_records_set & worldcat_records_set
        libraries.handle_file.set_to_csv(alma_worldcat_intersection,
            'records_in_both_sets',
            records_in_both_sets_writer,
            'OCLC Number')

        # Perform set difference: alma_records_set - worldcat_records_set
        alma_not_worldcat = alma_records_set - worldcat_records_set
        libraries.handle_file.set_to_csv(alma_not_worldcat,
            'records_in_alma_not_worldcat',
            records_in_alma_not_worldcat_writer,
            'OCLC Number')

        # Perform set difference: worldcat_records_set - alma_records_set
        worldcat_not_alma = worldcat_records_set - alma_records_set
        libraries.handle_file.set_to_csv(worldcat_not_alma,
            'records_in_worldcat_not_alma',
            records_in_worldcat_not_alma_writer,
            'OCLC Number')

    print(f'End of script. Completed in: {datetime.now() - start_time} ' \
        f'(hours:minutes:seconds.microseconds)')


if __name__ == "__main__":
    main()
