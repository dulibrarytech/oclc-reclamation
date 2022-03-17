import argparse
import dotenv
import libraries.record
import libraries.records_buffer
import logging
import logging.config
import os
import pandas as pd
from csv import writer
from datetime import datetime

dotenv_file = dotenv.find_dotenv()
dotenv.load_dotenv(dotenv_file)

logging.config.fileConfig(
    'logging.conf',
    defaults={'log_filename': f'logs/process_worldcat_records_'
        f'{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.log'},
    disable_existing_loggers=False)
logger = logging.getLogger(__name__)


def init_argparse() -> argparse.ArgumentParser:
    """Initializes and returns ArgumentParser object."""

    parser = argparse.ArgumentParser(
        description=('For each row in the input file, perform the specified '
            'operation (either get_current_oclc_number, set_holding, or '
            'unset_holding). Script results are saved to the following '
            'directory: outputs/process_worldcat_records/')
    )
    parser.add_argument(
        '-v', '--version', action='version',
        version=f'{parser.prog} version 1.0.0'
    )
    parser.add_argument(
        'operation',
        type=str,
        choices=['get_current_oclc_number', 'set_holding', 'unset_holding'],
        help=('the operation to be performed on each row of the input file '
            '(either get_current_oclc_number, set_holding, or unset_holding)'),
        metavar='operation'
    )
    parser.add_argument(
        'input_file',
        type=str,
        help=('the name and path of the file to be processed, which must be in '
            'CSV format (e.g. inputs/process_worldcat_records/set_holding/'
            'filename.csv)')
    )
    parser.add_argument(
        '--cascade',
        type=str,
        choices=['0', '1'],
        default='0',
        help=("only applicable to the unset_holding operation: whether or not "
            "to execute the operation if a local holdings record or local "
            "bibliographic record exists. Choose either 0 or 1 (default is 0). "
            "0 - don't unset holding if local holdings record or local "
            "bibliographic records exists; 1 - unset holding and delete local "
            "holdings record and local bibliographic record (if one exists)")
    )
    return parser


def main() -> None:
    """Performs the specified operation on every record in the input file.

    Gathers the maximum OCLC numbers possible before sending the appropriate
    request to the WorldCat Metadata API.

    Operations:
    - get_current_oclc_number
        For each row, check whether the given OCLC number is the current one.
        -- If so, then add the record to outputs/process_worldcat_records/
           get_current_oclc_number/already_has_current_oclc_number.csv
        -- If not, then add the record to outputs/process_worldcat_records/
           get_current_oclc_number/needs_current_oclc_number.csv
        -- If an error is encountered, then add the record to
           outputs/process_worldcat_records/get_current_oclc_number/
           records_with_errors_when_getting_current_oclc_number.csv

    - set_holding
        For each row, set holding for the given OCLC number.
        -- If holding is set successfully, then add the record to
           outputs/process_worldcat_records/set_holding/
           records_with_holding_successfully_set.csv
        -- If holding was already set, then add the record to
           outputs/process_worldcat_records/set_holding/
           records_with_holding_already_set.csv
        -- If an error is encountered, then add the record to
           outputs/process_worldcat_records/set_holding/
           records_with_errors_when_setting_holding.csv

    - unset_holding
        For each row, unset holding for the given OCLC number.
        -- If holding is unset successfully, then add the record to
           outputs/process_worldcat_records/unset_holding/
           records_with_holding_successfully_unset.csv
        -- If holding was already unset, then add the record to
           outputs/process_worldcat_records/unset_holding/
           records_with_holding_already_unset.csv
        -- If an error is encountered, then add the record to
           outputs/process_worldcat_records/unset_holding/
           records_with_errors_when_unsetting_holding.csv

    - If any of the above output files already exists in the directory, then it
      is appended to (not overwritten).
    """

    start_time = datetime.now()

    # Initialize parser and parse command-line args
    parser = init_argparse()
    args = parser.parse_args()

    # Convert input file into pandas DataFrame
    data = None
    if args.input_file.endswith('.csv'):
        data = pd.read_csv(args.input_file, dtype='str', keep_default_na=False)
    else:
        raise ValueError(f'Invalid format for input file ({args.input_file}). '
            f'Must be a CSV file (.csv).')

    command_line_args_str = (f'command-line args:\n'
        f'operation = {args.operation}\n'
        f'input_file = {args.input_file}\n'
        f'cascade = {args.cascade}')

    logger.info(f'Started {parser.prog} script with {command_line_args_str}')

    records_already_processed = set()
    results = None
    filename_for_records_to_update = None
    filename_for_records_with_no_update_needed = None
    filename_for_records_with_errors = None
    set_or_unset_choice = None

    if args.operation == 'get_current_oclc_number':
        results = {
            'num_records_with_current_oclc_num': 0,
            'num_records_with_old_oclc_num': 0,
            'num_records_with_errors': 0
        }
        filename_for_records_to_update = (
            'outputs/process_worldcat_records/get_current_oclc_number/'
            'needs_current_oclc_number.csv')
        filename_for_records_with_no_update_needed = (
            'outputs/process_worldcat_records/get_current_oclc_number/'
            'already_has_current_oclc_number.csv')
        filename_for_records_with_errors = (
            'outputs/process_worldcat_records/get_current_oclc_number/'
            'records_with_errors_when_getting_current_oclc_number.csv')
    else:
        results = {
            'num_records_updated': 0,
            'num_records_with_no_update_needed': 0,
            'num_records_with_errors': 0
        }

        if args.operation == 'set_holding':
            set_or_unset_choice = 'set'
        else:
            # args.operation == 'unset_holding'
            set_or_unset_choice = 'unset'

        filename_for_records_to_update = (
            f'outputs/process_worldcat_records/{set_or_unset_choice}_holding/'
            f'records_with_holding_successfully_{set_or_unset_choice}.csv')
        filename_for_records_with_no_update_needed = (
            f'outputs/process_worldcat_records/{set_or_unset_choice}_holding/'
            f'records_with_holding_already_{set_or_unset_choice}.csv')
        filename_for_records_with_errors = (
            f'outputs/process_worldcat_records/{set_or_unset_choice}_holding/'
            f'records_with_errors_when_{set_or_unset_choice}ting_holding.csv')

    records_buffer = None

    with open(filename_for_records_to_update, mode='a',
            newline='') as records_to_update, \
        open(filename_for_records_with_no_update_needed, mode='a',
            newline='') as records_with_no_update_needed, \
        open(filename_for_records_with_errors, mode='a',
            newline='') as records_with_errors:

        records_with_errors_writer = writer(records_with_errors)

        if args.operation == 'get_current_oclc_number':
            records_buffer = libraries.records_buffer.AlmaRecordsBuffer(
                records_with_no_update_needed,
                records_to_update,
                records_with_errors)
        else:
            records_buffer = libraries.records_buffer.WorldCatRecordsBuffer(
                set_or_unset_choice,
                args.cascade,
                records_with_no_update_needed,
                records_to_update,
                records_with_errors)

        # Loop over each row in DataFrame and check whether OCLC number is the
        # current one
        for index, row in data.iterrows():
            logger.debug(f'Started processing row {index + 2} of input file...')
            error_occurred = False
            error_msg = None

            try:
                mms_id = None
                orig_oclc_num = None
                if args.operation == 'get_current_oclc_number':
                    mms_id = row['MMS ID']
                    orig_oclc_num = \
                        row["Unique OCLC Number from Alma Record's 035 $a"]
                    mms_id = libraries.record.get_valid_record_identifier(
                        mms_id,
                        'MMS ID'
                    )
                else:
                    orig_oclc_num = row['OCLC Number']

                # Make sure OCLC Number is valid
                orig_oclc_num = libraries.record.get_valid_record_identifier(
                    orig_oclc_num, 'OCLC number')
                orig_oclc_num = \
                    libraries.record.remove_leading_zeros(orig_oclc_num)
                assert orig_oclc_num != '0', (f"Invalid OCLC number: "
                    f"'{orig_oclc_num}'. It cannot be zero.")

                if args.operation == 'get_current_oclc_number':
                    assert mms_id not in records_already_processed, (f'Record '
                        f'with MMS ID {mms_id} has already been processed.')
                    records_already_processed.add(mms_id)
                else:
                    assert orig_oclc_num not in records_already_processed, (
                        f'Record with OCLC Number {orig_oclc_num} has already '
                        f'been processed.')
                    records_already_processed.add(orig_oclc_num)

                if len(records_buffer) < int(os.environ[
                        'WORLDCAT_METADATA_API_MAX_RECORDS_PER_REQUEST']):
                    if args.operation == 'get_current_oclc_number':
                        records_buffer.add(orig_oclc_num, mms_id)
                    else:
                        records_buffer.add(orig_oclc_num)
                else:
                    # records_buffer has the maximum records possible per API
                    # request, so process these records
                    logger.debug('Records buffer is full.\n')
                    records_buffer.process_records(results)

                    # Now that its records have been processed, clear buffer
                    records_buffer.remove_all_records()

                    # Add current row's data to the empty buffer
                    if args.operation == 'get_current_oclc_number':
                        records_buffer.add(orig_oclc_num, mms_id)
                    else:
                        records_buffer.add(orig_oclc_num)
            except AssertionError as assert_err:
                if args.operation == 'get_current_oclc_number':
                    logger.exception(f"An assertion error occurred when "
                        f"processing MMS ID '{row['MMS ID']}' (at row "
                        f"{index + 2} of input file): {assert_err}")
                else:
                    logger.exception(f"An assertion error occurred when "
                        f"processing OCLC Number '{row['OCLC Number']}' (at "
                        f"row {index + 2} of input file): {assert_err}")
                error_msg = f"Assertion Error: {assert_err}"
                error_occurred = True
            finally:
                if error_occurred:
                    results['num_records_with_errors'] += 1

                    # Add record to records_with_errors spreadsheet
                    if args.operation == 'get_current_oclc_number':
                        if records_with_errors.tell() == 0:
                            # Write header row
                            records_with_errors_writer.writerow([
                                'MMS ID',
                                'OCLC Number',
                                'Error'
                            ])

                        records_with_errors_writer.writerow([
                            mms_id,
                            orig_oclc_num,
                            error_msg
                        ])
                    else:
                        if records_with_errors.tell() == 0:
                            # Write header row
                            records_with_errors_writer.writerow([
                                'Requested OCLC Number',
                                'New OCLC Number (if applicable)',
                                'Error'
                            ])

                        records_with_errors_writer.writerow([
                            orig_oclc_num,
                            '',
                            error_msg
                        ])
                logger.debug(f'Finished processing row {index + 2} of input '
                    f'file.\n')

        # If records_buffer is not empty, process remaining records
        if len(records_buffer) > 0:
            records_buffer.process_records(results)

    logger.info(f'Finished {parser.prog} script with {command_line_args_str}\n')

    print(f'End of script. Completed in: {datetime.now() - start_time} '
        f'(hours:minutes:seconds.microseconds).\n'
        f'The script made {records_buffer.num_api_requests_made} API '
        f'request(s).\n'
        f'Processed {len(data.index)} rows from input file:')

    if args.operation == 'get_current_oclc_number':
        print(f'- {results["num_records_with_current_oclc_num"]} record(s) '
            f'with current OCLC number\n'
            f'- {results["num_records_with_old_oclc_num"]} record(s) with '
            f'old OCLC number\n'
            f'- {results["num_records_with_errors"]} record(s) with errors')
    else:
        print(f'- {results["num_records_updated"]} record(s) updated, '
            f'i.e. holding was successfully {set_or_unset_choice}\n'
            f'- {results["num_records_with_no_update_needed"]} record(s) not '
            f'updated because holding was already {set_or_unset_choice}\n'
            f'- {results["num_records_with_errors"]} record(s) with errors\n')

if __name__ == "__main__":
    main()
