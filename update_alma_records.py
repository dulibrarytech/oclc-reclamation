import argparse
import libraries.record
import libraries.records_buffer
import logging
import logging.config
import os
import pandas as pd
import requests
from csv import writer
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


def int_in_range(value: str) -> int:
    """Returns the given value (as an int) if it's within range.

    Parameters
    ----------
    value: str
        The value to check

    Returns
    -------
    int
        The value as an integer

    Raises
    ------
    argparse.ArgumentTypeError
        If (1) the value cannot be converted to an integer or (2) the value is
        not within range
    """

    int_value = None

    try:
        int_value = int(value)
        assert 1 <= int_value <= int(os.environ[
            'ALMA_BIBS_API_MAX_RECORDS_PER_GET_REQUEST']
        )
    except (AssertionError, ValueError):
        raise argparse.ArgumentTypeError(f'{value} is not an integer between 1 '
            f'and '
            f'{int(os.environ["ALMA_BIBS_API_MAX_RECORDS_PER_GET_REQUEST"])} '
            f'(inclusive)')

    return int_value


def init_argparse() -> argparse.ArgumentParser:
    """Initializes and returns ArgumentParser object."""

    parser = argparse.ArgumentParser(
        description=('For each row in the input file, add the corresponding '
            'OCLC Number to the specified Alma record (indicated by the MMS '
            'ID). Script results are saved to the following directory: '
            'outputs/update_alma_records/')
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
            'update_alma_records/filename.csv)')
    )
    parser.add_argument(
        '--batch_size',
        type=int_in_range,
        default='1',
        help=(f'the number of records to batch together when making each GET '
            f'request to retrieve Alma records. Must be between 1 and '
            f'{int(os.environ["ALMA_BIBS_API_MAX_RECORDS_PER_GET_REQUEST"])}, '
            f'inclusive (default is 1). Larger batch sizes will result in '
            f'fewer total Alma API requests.')
    )
    return parser


def main() -> None:
    """Updates Alma records to have the corresponding OCLC number.

    For each row in the input file, the corresponding OCLC number is added to
    the specified Alma record (indicated by the MMS ID), unless the Alma record
    already contains that OCLC number. If the Alma record contains non-matching
    OCLC numbers in an 035 field (in the subfield $a), those OCLC numbers are
    moved to the 019 field (as long as they are valid).

    When processing each Alma record:
    - The original record is saved in XML format as:
      outputs/update_alma_records/xml/{mms_id}_original.xml
    - If the record is updated, then it is added to outputs/update_alma_records/
      records_updated.csv and the modified Alma record is saved in XML format
      as: outputs/update_alma_records/xml/{mms_id}_modified.xml
    - If the record is not updated because it already has the current OCLC
      number, then it is added to:
      outputs/update_alma_records/records_with_no_update_needed.csv
    - If an error is encountered, then the record is added to:
      outputs/update_alma_records/records_with_errors.csv
    - For the above output files, if an XML file with the same name already
      exists in the directory, then it is overwritten. If a CSV file with the
      same name already exists, then it is appended to.

    How OCLC numbers are recognized within an Alma record:
    - The 035 fields of the Alma record are checked.
    - For each 035 field, if the first subfield $a value begins with '(OCoLC)',
      'ocm', 'ocn', or 'on', then it is considered to be an OCLC number. Any
      subsequent subfield $a values within the same 035 field are logged (as a
      DEBUG-level event) but otherwise ignored.

    OCLC numbers (e.g. 'ocm01234567') consist of an optional prefix (e.g. 'ocm')
    and the number itself, which must contain only digits (e.g. '01234567').

    Extracting the OCLC number from the 035 $a value:
    - First, the '(OCoLC)' org code is removed (if present).
    - Then, this script searches for the first digit. If found, then everything
      before this first digit is considered the prefix, and everything after
      (and including) this first digit is considered the number itself.

    An OCLC number is considered valid if it falls into one of these categories:
    1) Number only (no prefix), i.e. only digits
    2) A number (i.e. only digits) preceded by one of the following valid
       prefixes:
       - ocm
       - ocn
       - on
       - |a (though not a traditional OCLC prefix, '|a' is allowed so this
         script can successfully process 035 $a values like '(OCoLC)|a01234567')
    3) Any value with a single trailing '#' character that would otherwise fall
       into one of the above categories, e.g. '01234567#' or 'ocm01234567#'

    How invalid OCLC numbers are handled:
    - Case 1 (invalid prefix, valid number): If the Alma record contains an OCLC
      number with an invalid prefix preceding a valid number, then this record
      is added to the records_with_errors.csv file and **not updated** in Alma.
      Example of a valid OCLC number with an invalid prefix (i.e. ABC):
      035 __ $a (OCoLC)ABC01234567
    - Case 2 (any or no prefix, invalid number): If the Alma record contains an
      OCLC number with no digits or at least one non-digit character after the
      first digit, then the entire 035 field is removed and the invalid OCLC
      number is not added to the 019 field. This record **would get updated** in
      Alma. Examples of invalid OCLC numbers:
      035 __ $a (OCoLC)01234567def
      035 __ $a (OCoLC)ocm01234567def
      035 __ $a (OCoLC)ABC01234567def
    """

    start_time = datetime.now()

    # Initialize parser and parse command-line args
    parser = init_argparse()
    args = parser.parse_args()

    # Convert input file into pandas DataFrame
    data = None
    if args.input_file.endswith('.csv'):
        data = pd.read_csv(args.input_file,
            dtype={'MMS ID': 'str', 'OCLC Number': 'str'},
            keep_default_na=False)
    elif args.input_file.endswith('.xlsx'):
        data = pd.read_excel(args.input_file, 'Sheet1', engine='openpyxl',
            dtype={'MMS ID': 'str', 'OCLC Number': 'str'},
            keep_default_na=False)
    elif args.input_file.endswith('.xls'):
        data = pd.read_excel(args.input_file, 'Sheet1', engine='xlrd',
            dtype={'MMS ID': 'str', 'OCLC Number': 'str'},
            keep_default_na=False)
    else:
        raise ValueError(f'Invalid format for input file ({args.input_file}). '
            f'Must be one of the following file formats: CSV (.csv) or Excel '
            f'(.xlsx or .xls).')

    # Configure logging
    logging.config.fileConfig(
        'logging.conf',
        defaults={'log_filename': f'logs/update_alma_records_'
            f'{start_time.strftime("%Y-%m-%d_%H-%M-%S")}.log'},
        disable_existing_loggers=False
    )

    command_line_args_str = (f'command-line args:\n'
        f'input_file = {args.input_file}\n'
        f'batch_size = {args.batch_size}')

    logger.info(f'Started {parser.prog} script with {command_line_args_str}')

    records_buffer = None
    records_already_processed = set()

    # Loop over rows in DataFrame and update the corresponding Alma record
    with open('outputs/update_alma_records/records_updated.csv', mode='a',
            newline='') as records_updated, \
        open('outputs/update_alma_records/records_with_no_update_needed.csv',
            mode='a', newline='') as records_with_no_update_needed, \
        open('outputs/update_alma_records/records_with_errors.csv', mode='a',
            newline='') as records_with_errors:

        records_with_errors_writer = writer(records_with_errors)

        records_buffer = libraries.records_buffer.AlmaRecordsBuffer(
            records_updated,
            records_with_no_update_needed,
            records_with_errors
        )

        for index in range(len(data.index) + 1):
            raw_mms_id = None
            mms_id = None

            raw_oclc_num = None
            oclc_num = None

            error_occurred = False
            error_msg = None

            row_location = None
            batch_name = None
            batch_level_error = True

            try:
                if index < len(data.index):
                    row_location = f'at row {index + 2} of input file'

                    # Make sure that mms_id and oclc_num are valid
                    raw_mms_id = data.at[index, 'MMS ID']
                    raw_oclc_num = data.at[index, 'OCLC Number']
                    batch_name = (f"batch ending with MMS ID '{raw_mms_id}' "
                        f"({row_location})")

                    mms_id = libraries.record.get_valid_record_identifier(
                        raw_mms_id,
                        'MMS ID'
                    )
                    oclc_num = libraries.record.get_valid_record_identifier(
                        raw_oclc_num,
                        'OCLC number'
                    )

                    # Remove leading zeros from OCLC Number
                    oclc_num = libraries.record.remove_leading_zeros(oclc_num)

                    assert mms_id not in records_already_processed, (f'Record '
                        f'with MMS ID {mms_id} either (1) has already been '
                        f'processed or (2) has already been added to the '
                        f'current batch.')
                    records_already_processed.add(mms_id)

                    if len(records_buffer) < args.batch_size:
                        records_buffer.add(mms_id, oclc_num)

                    # If records buffer is full, process these records
                    if len(records_buffer) == args.batch_size:
                        logger.debug(f'Records buffer is full ({row_location}).'
                            f'\n')
                        records_buffer.process_records()
                else:
                    # End of DataFrame has been reached.
                    row_location = 'after processing final row of input file'
                    batch_name = 'final batch'

                    # If records buffer is not empty, process the remaining
                    # records.
                    if len(records_buffer) > 0:
                        records_buffer.process_records()
            except AssertionError as assert_err:
                logger.exception(f'An assertion error occurred: {assert_err}')
                error_msg = f'Assertion Error: {assert_err}'
                batch_level_error = error_msg.startswith(
                    'Assertion Error: Not enough Alma Daily API Requests'
                )
                error_occurred = True
            except requests.exceptions.ConnectionError as connection_err:
                logger.exception(f'A second error occurred (Connection Error) '
                    f'when trying to retrieve Alma record(s): {connection_err}')

                error_msg = f'Connection Error: {connection_err}'
                error_occurred = True
            except requests.exceptions.HTTPError as http_err:
                logger.exception(f'A second error occurred (HTTP Error) when '
                    f'trying to retrieve Alma record(s): {http_err}')

                if (hasattr(http_err, 'response')
                        and hasattr(http_err.response, 'text')):
                    logger.error(f'API Response:\n{http_err.response.text}')

                error_msg = f'HTTP Error: {http_err}'
                error_occurred = True
            except KeyError as key_err:
                logger.exception(f'A key error occurred: Unable to access the '
                    f'{key_err} key.')

                if str(key_err).strip("'") in {'MMS ID', 'OCLC Number'}:
                    error_msg = (f'Input file must contain a column named: '
                        f'{key_err}')
                    logger.error(error_msg)
                    error_msg = f'Key Error: {error_msg}'
                    batch_level_error = False
                else:
                    error_msg = f'Key Error: Unable to access the {key_err} key'

                error_occurred = True
            except Exception as err:
                logger.exception(f'An error occurred: {err}')
                error_msg = f'{err}'
                error_occurred = True
            finally:
                if error_occurred:
                    if args.batch_size > 1 and batch_level_error:
                        records_buffer.num_records_with_errors += len(
                            records_buffer
                        )

                        action = ('retrieving batched Alma record(s)'
                            if error_msg.startswith('HTTP Error')
                            else 'processing batched Alma record(s)')

                        mms_ids_in_batch = '\n'.join(
                            records_buffer.mms_id_to_oclc_num_dict.keys()
                        )

                        # Log where the error occurred
                        logger.error(f'This error occurred when {action} for '
                            f'the {batch_name}.\n'
                            f'MMS ID(s) in batch:\n{mms_ids_in_batch}\n')

                        # Add each record in batch to records_with_errors
                        # spreadsheet
                        for batch_index, (
                                record_mms_id,
                                record_oclc_num) in enumerate(
                                records_buffer.mms_id_to_oclc_num_dict.items(),
                                start=1):
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
                                record_mms_id,
                                '<record not fully checked>',
                                record_oclc_num,
                                (f'Error {action} (record #{batch_index} of '
                                    f'{len(records_buffer)} in batch): '
                                    f'{error_msg}')
                            ])
                    else:
                        records_buffer.num_records_with_errors += 1

                        # Log where the error occurred
                        if index < len(data.index):
                            logger.error(f"This error occurred when processing "
                                f"MMS ID '{raw_mms_id}' ({row_location}).\n")
                        else:
                            logger.error(f'This error occurred {row_location}.'
                                '\n')

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
                            (mms_id
                                if mms_id is not None
                                else raw_mms_id),
                            '<record not fully checked>',
                            (oclc_num
                                if oclc_num is not None
                                else raw_oclc_num),
                            error_msg
                        ])

                    # Stop processing records if one of the following errors
                    # occur
                    if error_msg.startswith((
                            'Assertion Error: Not enough Alma Daily API Requests',
                            'Connection Error',
                            'HTTP Error',
                            'Key Error: Input file must contain a column'
                            )):
                        logger.error('Halting script because of above error.\n')
                        break

                # If records buffer is full, clear buffer (now that its records
                # have been processed)
                if len(records_buffer) == args.batch_size:
                    records_buffer.remove_all_records()

    logger.info(f'Finished {parser.prog} script with {command_line_args_str}\n')

    logger.info(f'Script completed in: {datetime.now() - start_time} '
        f'(hours:minutes:seconds.microseconds).\n')

    logger.info(f'The script made {records_buffer.num_api_requests_made} API '
        f'request(s).\n')

    if records_buffer.num_api_requests_remaining is not None:
        logger.info(f'Alma API requests remaining for today: '
            f'{records_buffer.num_api_requests_remaining}\n')

    total_records_in_output_files = (records_buffer.num_records_updated
        + records_buffer.num_records_with_no_update_needed
        + records_buffer.num_records_with_errors)

    logger.info(f'Processed {total_records_in_output_files} of '
        f'{len(data.index)} row(s) from input file:\n'
        f'- {records_buffer.num_records_updated} record(s) updated\n'
        f'- {records_buffer.num_records_with_no_update_needed} record(s) with '
        f'no update needed\n'
        f'- {records_buffer.num_records_with_errors} record(s) with errors\n')

    assert len(data.index) == total_records_in_output_files, (f'Total records '
        f'in input file ({len(data.index)}) does not equal total records in '
        f'output files ({total_records_in_output_files})\n')


if __name__ == "__main__":
    main()
