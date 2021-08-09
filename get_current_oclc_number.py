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
from typing import Dict

load_dotenv()

logging.config.fileConfig('logging.conf', disable_existing_loggers=False)
logger = logging.getLogger(__name__)


def get_current_oclc_numbers(mms_id: str, oclc_num: str):
    """GETs the current OCLC number for the given record.

    Sends a GET request to the WorldCat Metadata API:
    https://worldcat.org/bib/checkcontrolnumbers?oclcNumbers={oclcNumbers}

    Parameters
    ----------
    mms_id: str
        The MMS ID of the Alma record
    oclc_num: str
        The OCLC number of the record

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
        f"/bib/checkcontrolnumbers?oclcNumbers={oclc_num}&transactionID="
        f"{transactionID}", headers=headers, timeout=45)
    libraries.api.log_response_and_raise_for_status(response)
    logger.debug(f'{response.text=}')

    return response.json()


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

    Gathers up to 50 OCLC numbers before sending a GET request.
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
    else:
        logger.exception(f'Invalid format for input file ({args.Input_file}). '
            f'Input file must a CSV file (.csv)')
        return

    # Loop over rows in DataFrame and check with OCLC number is the current one
    num_records_processed = 0
    num_records_with_current_oclc_num = 0
    num_records_with_old_oclc_num = 0
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

        for index, row in data.iterrows():
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

                result = get_current_oclc_numbers(mms_id, orig_oclc_num)
                logger.debug(f'{type(result)=}')
                api_response_error_msg = (f"Problem with Get Current OCLC "
                    f"Number API request for OCLC Number '{orig_oclc_num}' "
                    f"(MMS ID '{mms_id}') at row {index + 2} of input file: ")

                if result is None:
                    error_msg = 'No JSON response'
                    logger.debug(api_response_error_msg + error_msg)
                else:
                    num_records_processed += 1
                    logger.debug(f'Result of Get Current OCLC Number request:\n'
                        f'{json.dumps(result, indent=2)}')

                    found_requested_oclc_num = result['entry'][0]['found']
                    is_current_oclc_num = not result['entry'][0]['merged']
                    logger.debug(f'{is_current_oclc_num=}')
                    logger.debug(f'{type(is_current_oclc_num)=}')

                    if not found_requested_oclc_num:
                        error_msg = 'OCLC number not found'
                        logger.debug(api_response_error_msg + error_msg)
                    elif is_current_oclc_num:
                        error_occurred = False
                        num_records_with_current_oclc_num += 1

                        # Add record to already_has_current_oclc_number.csv
                        if records_with_current_oclc_num.tell() == 0:
                            # Write header row
                            records_with_current_oclc_num_writer.writerow([
                                'MMS ID',
                                'Current OCLC Number'
                            ])

                        records_with_current_oclc_num_writer.writerow([
                            mms_id,
                            orig_oclc_num
                        ])
                    else:
                        error_occurred = False
                        num_records_with_old_oclc_num += 1

                        current_oclc_num = \
                            result['entry'][0]['currentOclcNumber']

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
                            current_oclc_num,
                            orig_oclc_num
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
                            'OCLC Number',
                            'Error'
                        ])

                    records_with_errors_writer.writerow([
                        mms_id,
                        orig_oclc_num,
                        error_msg
                    ])

    print(f'\nEnd of script. Processed {num_records_processed} records out of '
        f'{len(data.index)} rows from input file:\n'
        f'- {num_records_with_current_oclc_num} record(s) with current OCLC '
        f'number\n- {num_records_with_old_oclc_num} record(s) with old OCLC '
        f'number')


if __name__ == "__main__":
    main()
