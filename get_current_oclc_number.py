import argparse
import json
import libraries.api
import libraries.record
import logging
import logging.config
import os
import pandas as pd
import requests
import time
from csv import writer
from dotenv import load_dotenv
from requests.exceptions import HTTPError
from typing import Dict, TextIO

load_dotenv()

logging.config.fileConfig('logging.conf', disable_existing_loggers=False)
logger = logging.getLogger(__name__)


class RecordsBuffer:
    """
    A buffer of records, each of whose OCLC number needs to be checked.

    Attributes
    ----------
    lookup_table: Dict[str, str]
        A dictionary containing each record's original OCLC number (key) and its
        MMS ID (value)
    records_with_current_oclc_num: TextIO
        The CSV file object where records with a current OCLC number are added
    records_with_old_oclc_num: TextIO
        The CSV file object where records with an old OCLC number are added
    records_with_errors: TextIO
        The CSV file object where records are added if an error is encountered

    Methods
    -------
    process_json_response(json_response)
        Processes the JSON response from the get_current_oclc_numbers function
    """

    def __init__(self,
            records_with_current_oclc_num: TextIO,
            records_with_old_oclc_num: TextIO,
            records_with_errors: TextIO) -> None:
        logger.debug('Started RecordsBuffer constructor...')

        self.records_buffer = {}
        logger.debug(f'{type(self.records_buffer)=}')

        self.records_with_current_oclc_num = records_with_current_oclc_num
        self.records_with_current_oclc_num_writer = \
            writer(records_with_current_oclc_num)
        logger.debug(f'{type(self.records_with_current_oclc_num)=}')
        logger.debug(f'{type(self.records_with_current_oclc_num_writer)=}')

        self.records_with_old_oclc_num = records_with_old_oclc_num
        self.records_with_old_oclc_num_writer = \
            writer(records_with_old_oclc_num)

        self.records_with_errors = records_with_errors
        self.records_with_errors_writer = writer(records_with_errors)

        logger.debug('Completed RecordsBuffer constructor.')


def get_current_oclc_numbers(oclc_nums: str):
    """GETs the current OCLC number for each number in oclc_nums.

    Sends a GET request to the WorldCat Metadata API:
    https://worldcat.org/bib/checkcontrolnumbers?oclcNumbers={oclcNumbers}

    Parameters
    ----------
    oclc_nums: str
        The OCLC numbers to be checked. Each OCLC number should be separated by
        a comma and contain no spaces, leading zeros, or non-digit characters.

    Returns
    -------
    Dict
        The JSON response object
    """

    token = os.getenv('WORLDCAT_METADATA_API_TOKEN')
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    transactionID = f"DVP_{timestamp}_{os.getenv('WORLDCAT_PRINCIPAL_ID')}"

    response = requests.get(f"{os.getenv('WORLDCAT_METADATA_SERVICE_URL')}"
        f"/bib/checkcontrolnumbers?oclcNumbers={oclc_nums}&transactionID="
        f"{transactionID}", headers=headers, timeout=45)
    logger.debug(f'{type(response)=}')
    libraries.api.log_response_and_raise_for_status(response)

    return response.json()


def process_json_response(json_response, records_buffer, results,
        records_with_current_oclc_num, records_with_current_oclc_num_writer,
        records_with_old_oclc_num, records_with_old_oclc_num_writer):
    """Processes the JSON response from the get_current_oclc_numbers function.

    Parameters
    ----------
    json_response: Dict
        The JSON response returned by the get_current_oclc_numbers function
    records_buffer: Dict
        The initial dictionary, which will be updated with data from the
        json_response

    Returns
    -------
    Dict
        A dictionary with the MMS ID of each record as the key, and a nested
        dictionary (containing the remaining record data) as the value
    """
    api_response_error_msg = ('Problem with Get Current OCLC Number API '
        'request')

    if json_response is None:
        logger.debug(f'{api_response_error_msg}: No JSON response')
    else:
        logger.debug(f'Result of Get Current OCLC Number request:\n'
            f'{json.dumps(json_response, indent=2)}')

        for record_index, record in enumerate(json_response['entry'],
                start=1):
            found_requested_oclc_num = record['found']
            is_current_oclc_num = not record['merged']

            # Look up MMS ID based on OCLC number
            mms_id = records_buffer[record['requestedOclcNumber']]

            logger.debug(f'Started processing record #{record_index} (OCLC '
                f'number {record["requestedOclcNumber"]})...')
            logger.debug(f'{is_current_oclc_num=}')
            logger.debug(f'{type(is_current_oclc_num)=}')

            if not found_requested_oclc_num:
                logger.exception(f'{api_response_error_msg}: OCLC number '
                    f'{record["requestedOclcNumber"]} not found')
            elif is_current_oclc_num:
                error_occurred = False
                results['num_records_with_current_oclc_num'] += 1

                # Add record to already_has_current_oclc_number.csv
                if records_with_current_oclc_num.tell() == 0:
                    # Write header row
                    records_with_current_oclc_num_writer.writerow([
                        'MMS ID',
                        'Current OCLC Number'
                    ])

                records_with_current_oclc_num_writer.writerow([
                    mms_id,
                    record['currentOclcNumber']
                ])
            else:
                error_occurred = False
                results['num_records_with_old_oclc_num'] += 1

                # Add record to needs_current_oclc_number.csv
                if records_with_old_oclc_num.tell() == 0:
                    # Write header row
                    records_with_old_oclc_num_writer.writerow([
                        'MMS ID',
                        'Current OCLC Number',
                        'Original OCLC Number'
                    ])

                records_with_old_oclc_num_writer.writerow([
                    mms_id,
                    record['currentOclcNumber'],
                    record['requestedOclcNumber']
                ])
            logger.debug(f'Finished processing record #{record_index}.\n')


def process_records_buffer(records_buffer: Dict[str, str],
        results: Dict[str, int], records_with_current_oclc_num,
        records_with_current_oclc_num_writer, records_with_old_oclc_num,
        records_with_old_oclc_num_writer):
    """Updates the given records_buffer with each record's current OCLC number.

    Parameters
    ----------
    records_buffer: Dict[str, str]
        The initial dictionary with the MMS ID of each record as the key, and
        the original OCLC number as the value

    Returns
    -------
    Dict
        The updated dictionary with the MMS ID of each record as the key, and a
        nested dictionary (containing record data such as the current OCLC
        number) as the value
    """

    json_response = get_current_oclc_numbers(','.join(records_buffer.keys()))
    logger.debug(f'{type(json_response)=}')
    return process_json_response(
        json_response,
        records_buffer,
        results,
        records_with_current_oclc_num,
        records_with_current_oclc_num_writer,
        records_with_old_oclc_num,
        records_with_old_oclc_num_writer
    )


def init_argparse() -> argparse.ArgumentParser:
    """Initializes and returns ArgumentParser object."""

    parser = argparse.ArgumentParser(
        usage='%(prog)s [option] input_file',
        description=('For each row in the input file, check whether the given '
            'OCLC number is the current one.')
    )
    parser.add_argument(
        '-v', '--version', action='version',
        version=f'{parser.prog} version 1.0.0'
    )
    parser.add_argument(
        'Input_file',
        metavar='input_file',
        type=str,
        help=('the name and path of the input file, which must be in CSV '
            'format (e.g. '
            'csv/master_list_records_with_potentially_old_oclc_num.csv)')
    )
    return parser


def main() -> None:
    """Gets the current OCLC number for every record in input file.

    For each row, check whether the given OCLC number is the current one:
    - If so, then add the record to already_has_current_oclc_number.csv
    - If not, then add the record to needs_current_oclc_number.csv

    Gathers the maximum OCLC numbers possible before sending a GET request.
    """

    # Initialize parser and parse command-line args
    parser = init_argparse()
    args = parser.parse_args()

    # Convert input file into pandas DataFrame
    data = None
    if args.Input_file.endswith('.csv'):
        data = pd.read_csv(args.Input_file, dtype='str', keep_default_na=False)
    else:
        logger.exception(f'Invalid format for input file ({args.Input_file}). '
            f'Input file must a CSV file (.csv)')
        return

    results = {
        'num_records_with_current_oclc_num': 0,
        'num_records_with_old_oclc_num': 0,
        'num_records_with_errors': 0
    }

    with open('csv/needs_current_oclc_number.csv', mode='a',
            newline='') as records_with_old_oclc_num, \
        open('csv/already_has_current_oclc_number.csv', mode='a',
            newline='') as records_with_current_oclc_num, \
        open('csv/records_with_errors_when_getting_current_oclc_number.csv',
            mode='a', newline='') as records_with_errors:

        records_with_old_oclc_num_writer = writer(records_with_old_oclc_num)
        records_with_current_oclc_num_writer = \
            writer(records_with_current_oclc_num)
        records_with_errors_writer = writer(records_with_errors)

        records_buffer = {}
        test = RecordsBuffer(records_with_current_oclc_num,
            records_with_old_oclc_num, records_with_errors)
        logger.debug(f'{type(test)=}')

        # Loop over each row in DataFrame and check whether OCLC number is the
        # current one
        for index, row in data.iterrows():
            logger.debug(f'Started processing row {index + 2} of input file...')
            error_occurred = True
            error_msg = None
            result = None

            try:
                mms_id = row['MMS ID']
                orig_oclc_num = \
                    row["Unique OCLC Number from Alma Record's 035 $a"]

                # Make sure that mms_id and orig_oclc_num are valid
                mms_id = libraries.record.get_valid_record_identifier(mms_id,
                    'MMS ID')
                orig_oclc_num = libraries.record.get_valid_record_identifier(
                    orig_oclc_num, 'OCLC number')
                orig_oclc_num = \
                    libraries.record.remove_leading_zeros(orig_oclc_num)

                if len(records_buffer) < int(os.getenv(
                        'WORLDCAT_METADATA_API_MAX_RECORDS_PER_REQUEST')):
                    assert orig_oclc_num not in records_buffer, (f'OCLC number '
                        f'{orig_oclc_num} already found earlier in input file.')
                    records_buffer[orig_oclc_num] = mms_id
                    logger.debug(f'Added {orig_oclc_num} to records_buffer')
                    logger.debug(f'{records_buffer=}')
                else:
                    # records_buffer has the maximum records possible per API
                    # request
                    process_records_buffer(
                        records_buffer,
                        results,
                        records_with_current_oclc_num,
                        records_with_current_oclc_num_writer,
                        records_with_old_oclc_num,
                        records_with_old_oclc_num_writer
                    )

                    # Now that its records have been processed, clear buffer
                    records_buffer.clear()

                    # Add current row's data to the empty records_buffer
                    records_buffer[orig_oclc_num] = mms_id
                    logger.debug(f'Added {orig_oclc_num} to records_buffer')
                    logger.debug(f'{records_buffer=}')

                error_occurred = False
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
                    results['num_records_with_errors'] += 1

                    # Add record to records_with_errors spreadsheet
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
                logger.debug(f'Finished processing row {index + 2} of input '
                    f'file.\n')

        # If records_buffer is not empty, process remaining records
        if len(records_buffer) > 0:
            process_records_buffer(
                records_buffer,
                results,
                records_with_current_oclc_num,
                records_with_current_oclc_num_writer,
                records_with_old_oclc_num,
                records_with_old_oclc_num_writer
            )

    print(f'\nEnd of script. Processed {len(data.index)} rows from input file:'
        f'\n- {results["num_records_with_current_oclc_num"]} record(s) with current OCLC '
        f'number\n- {results["num_records_with_old_oclc_num"]} record(s) with old OCLC '
        f'number\n- {results["num_records_with_errors"]} record(s) with errors')


if __name__ == "__main__":
    main()
