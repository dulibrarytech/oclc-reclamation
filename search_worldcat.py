import argparse
import dotenv
import libraries.record
import libraries.records_buffer
import logging
import logging.config
import pandas as pd
from datetime import datetime

logging.config.fileConfig('logging.conf', disable_existing_loggers=False)
logger = logging.getLogger(__name__)


def init_argparse() -> argparse.ArgumentParser:
    """Initializes and returns ArgumentParser object."""

    parser = argparse.ArgumentParser(
        usage='%(prog)s [option] input_file',
        description=("For each row in the input file, find the record's OCLC "
            "Number by searching WorldCat using the available record "
            "identifiers. Then update the input file accordingly.")
    )
    parser.add_argument(
        '-v', '--version', action='version',
        version=f'{parser.prog} version 1.0.0'
    )
    parser.add_argument(
        'input_file',
        type=str,
        help=('the name and path of the input file, which must be in either '
            'CSV (.csv) or Excel (.xlsx or .xls) format (e.g. inputs/'
            'search_worldcat/filename.csv)')
    )
    return parser


def main() -> None:
    """Searches WorldCat for each record in input file and saves OCLC Number.

    For each row in the input file, a WorldCat search is performed using each
    available record identifier (LCCN, ISBN, ISSN, and Government Document
    Number), stopping as soon as search results are found for a given
    identifier.

    The following columns of the input file are then updated:
    - OCLC Number
        If the search returns one record, its OCLC Number is added here
    - OCLC Number possibilities
        If the search returns multiple records, each record's OCLC Number is
        added here (separated by a comma)
    - Multiple OCLC Numbers?
        A value of "1" is added if the search returns multiple records;
        otherwise, this field will be blank (and any existing value will be
        removed)
    - Error
        If any errors are encountered, relevant error info is added here
    """

    start_time = datetime.now()

    # Initialize parser and parse command-line args
    parser = init_argparse()
    args = parser.parse_args()

    # Convert input file into pandas DataFrame
    data = None
    if args.input_file.endswith('.csv'):
        data = pd.read_csv(args.input_file,
            dtype=str,
            keep_default_na=False)
    elif args.input_file.endswith('.xlsx'):
        data = pd.read_excel(args.input_file, 'Sheet1', engine='openpyxl',
            dtype=str,
            keep_default_na=False)
    elif args.input_file.endswith('.xls'):
        data = pd.read_excel(args.input_file, 'Sheet1', engine='xlrd',
            dtype=str,
            keep_default_na=False)
    else:
        raise ValueError(f'Invalid format for input file ({args.input_file}). '
            f'Must be one of the following file formats: CSV (.csv) or Excel '
            f'(.xlsx or .xls).')

    records_already_processed = set()
    logger.debug(f'{records_already_processed=}\n')

    results = {
        'num_records_with_single_search_result': 0,
        'num_records_with_multiple_search_results': 0,
        'num_records_with_errors': 0
    }

    records_buffer = libraries.records_buffer.WorldCatSearchBuffer(data)

    # Loop over rows in DataFrame
    for row in data.itertuples(name='Record_from_input_file'):
        logger.debug(f'Started processing row {row.Index + 2} of input file...')
        logger.debug(row)
        error_occurred = False
        error_msg = None

        try:
            mms_id = libraries.record.get_valid_record_identifier(
                row.mms_id,
                'MMS ID'
            )

            assert mms_id not in records_already_processed, (f'Record with MMS '
                f'ID {mms_id} has already been processed.')
            records_already_processed.add(mms_id)

            assert len(records_buffer) == 0, (f'Records buffer was not '
                f'properly emptied. It currently contains '
                f'{len(records_buffer)} record(s).')

            # Add current row's data to the empty buffer and process that record
            records_buffer.add(row)
            records_buffer.process_records(results)
        except AssertionError as assert_err:
            logger.exception(f"An assertion error occurred when processing MMS "
                f"ID '{row.mms_id}' (at row {row.Index + 2} of input file): "
                f"{assert_err}")
            error_msg = f"Assertion Error: {assert_err}"
            error_occurred = True
        finally:
            if error_occurred:
                results['num_records_with_errors'] += 1

                # Update Error column of input file for the given row
                data.loc[row.Index, 'error'] = error_msg

            logger.debug(f"After processing row {row.Index + 2}:\n"
                f"{data.loc[row.Index]}")

            logger.debug(f'Finished processing row {row.Index + 2} of input '
                f'file.\n')

            # Now that row has been processed, clear buffer
            records_buffer.remove_all_records()

    logger.debug(f'Updated DataFrame:\n{data}\n')

    logger.debug(f"Updated DataFrame (selected columns):\n"
        f"{data[['mms_id', 'oclc_num', 'found_multiple_oclc_nums', 'lccn', 'error']]}\n")

    print(f'End of script. Completed in: {datetime.now() - start_time} ' \
        f'(hours:minutes:seconds.microseconds)')


if __name__ == "__main__":
    main()
