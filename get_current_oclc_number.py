import argparse
import dotenv
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
from oauthlib.oauth2 import BackendApplicationClient, TokenExpiredError
from requests.auth import HTTPBasicAuth
from requests_oauthlib import OAuth2Session
from typing import Dict, TextIO

dotenv_file = dotenv.find_dotenv()
dotenv.load_dotenv(dotenv_file)

logging.config.fileConfig('logging.conf', disable_existing_loggers=False)
logger = logging.getLogger(__name__)


class RecordsBuffer:
    """
    A buffer of records, each of whose OCLC number needs to be checked.

    Attributes
    ----------
    auth:
    oauth_session:
    oclc_num_dict: Dict[str, str]
        A dictionary containing each record's original OCLC number (key) and its
        MMS ID (value)
    records_with_current_oclc_num: TextIO
        The CSV file object where records with a current OCLC number are added
    records_with_current_oclc_num_writer: writer
        The CSV writer object for the records_with_current_oclc_num file object
    records_with_old_oclc_num: TextIO
        The CSV file object where records with an old OCLC number are added
    records_with_old_oclc_num_writer: writer
        The CSV writer object for the records_with_old_oclc_num file object
    records_with_errors: TextIO
        The CSV file object where records are added if an error is encountered
    records_with_errors_writer: writer
        The CSV writer object for the records_with_errors file object

    Methods
    -------
    add(orig_oclc_num, mms_id)
        Adds the given record to this buffer (i.e. to oclc_num_dict)
    process_api_response(api_response, results)
        Processes the API response from the check_oclc_numbers function
    process_records(results)
        Checks each record in oclc_num_dict for the current OCLC number
    remove_all_records()
        Removes all records from this buffer (i.e. clears oclc_num_dict)
    """

    def __init__(self,
            records_with_current_oclc_num: TextIO,
            records_with_old_oclc_num: TextIO,
            records_with_errors: TextIO) -> None:
        """Instantiates a RecordsBuffer object.

        Parameters
        ----------
        records_with_current_oclc_num: TextIO
            The CSV file object where records with a current OCLC number are
            added
        records_with_old_oclc_num: TextIO
            The CSV file object where records with an old OCLC number are added
        records_with_errors: TextIO
            The CSV file object where records are added if an error is
            encountered
        """

        logger.debug('Started RecordsBuffer constructor...')

        self.oclc_num_dict = {}
        logger.debug(f'{type(self.oclc_num_dict)=}')

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

        # Create OAuth2Session for WorldCat Metadata API
        logger.debug('Creating OAuth2Session...')
        self.auth = HTTPBasicAuth(os.getenv('WORLDCAT_METADATA_API_KEY'),
            os.getenv('WORLDCAT_METADATA_API_SECRET'))
        client = BackendApplicationClient(
            client_id=os.getenv('WORLDCAT_METADATA_API_KEY'),
            scope=['WorldCatMetadataAPI refresh_token'])
        token = {
            'access_token': os.getenv('WORLDCAT_METADATA_API_ACCESS_TOKEN'),
            'expires_at': float(os.getenv(
                'WORLDCAT_METADATA_API_ACCESS_TOKEN_EXPIRES_AT')),
            'token_type': os.getenv('WORLDCAT_METADATA_API_ACCESS_TOKEN_TYPE')
            }
        self.oauth_session = OAuth2Session(client=client, token=token)
        logger.debug(f'{type(self.oauth_session)=}')
        logger.debug('OAuth2Session created.')

        logger.debug('Completed RecordsBuffer constructor.\n')

    def __str__(self) -> str:
        """Returns a string listing the contents of the OCLC Number dictionary.

        Returns
        -------
        str
            The contents of the OCLC Number dictionary
        """

        return (f'Records buffer contents ({{OCLC Number: MMS ID}}): '
            f'{self.oclc_num_dict}')

    def add(self, orig_oclc_num: str, mms_id: str) -> None:
        """Adds the given record to this buffer (i.e. to oclc_num_dict).

        Parameters
        ----------
        orig_oclc_num: str
            The record's original OCLC number
        mms_id: str
            The record's MMS ID

        Raises
        ------
        AssertionError
            If the original OCLC number is already in the OCLC Number dictionary
        """

        assert orig_oclc_num not in self.oclc_num_dict, (f'OCLC number '
            f'{orig_oclc_num} already exists in records buffer with MMS ID '
            f'{self.oclc_num_dict[orig_oclc_num]}')
        self.oclc_num_dict[orig_oclc_num] = mms_id
        logger.debug(f'Added {orig_oclc_num} to records buffer.')
        logger.debug(self.__str__())

    def process_api_response(self, api_response: requests.models.Response,
            results: Dict[str, int]) -> None:
        """Processes the API response from the check_oclc_numbers function.

        Parameters
        ----------
        api_response: requests.models.Response
            The API response returned by the check_oclc_numbers function
        results: Dict[str, int]
            A dictionary containing the total number of records in the following
            categories: records with the current OCLC number, records with an
            old OCLC number, records with errors

        Raises
        ------
        json.decoder.JSONDecodeError
            If there is an error decoding the API response
        """

        api_response_error_msg = ('Problem with Get Current OCLC Number API '
            'response')

        try:
            json_response = api_response.json()
            logger.debug(f'Get Current OCLC Number API response:\n'
                f'{json.dumps(json_response, indent=2)}')

            for record_index, record in enumerate(json_response['entry'],
                    start=1):
                found_requested_oclc_num = record['found']
                is_current_oclc_num = not record['merged']

                # Look up MMS ID based on OCLC number
                mms_id = self.oclc_num_dict[record['requestedOclcNumber']]

                logger.debug(f'Started processing record #{record_index} (OCLC '
                    f'number {record["requestedOclcNumber"]})...')
                logger.debug(f'{is_current_oclc_num=}')

                if not found_requested_oclc_num:
                    logger.exception(f'{api_response_error_msg}: OCLC number '
                        f'{record["requestedOclcNumber"]} not found')

                    results['num_records_with_errors'] += 1

                    # Add record to records_with_errors spreadsheet
                    if self.records_with_errors.tell() == 0:
                        # Write header row
                        self.records_with_errors_writer.writerow([
                            'MMS ID',
                            'OCLC Number',
                            'Error'
                        ])

                    self.records_with_errors_writer.writerow([
                        mms_id,
                        record['requestedOclcNumber'],
                        f'{api_response_error_msg}: OCLC number not found'
                    ])
                elif is_current_oclc_num:
                    results['num_records_with_current_oclc_num'] += 1

                    # Add record to already_has_current_oclc_number.csv
                    if self.records_with_current_oclc_num.tell() == 0:
                        # Write header row
                        self.records_with_current_oclc_num_writer.writerow([
                            'MMS ID',
                            'Current OCLC Number'
                        ])

                    self.records_with_current_oclc_num_writer.writerow([
                        mms_id,
                        record['currentOclcNumber']
                    ])
                else:
                    results['num_records_with_old_oclc_num'] += 1

                    # Add record to needs_current_oclc_number.csv
                    if self.records_with_old_oclc_num.tell() == 0:
                        # Write header row
                        self.records_with_old_oclc_num_writer.writerow([
                            'MMS ID',
                            'Current OCLC Number',
                            'Original OCLC Number'
                        ])

                    self.records_with_old_oclc_num_writer.writerow([
                        mms_id,
                        record['currentOclcNumber'],
                        record['requestedOclcNumber']
                    ])
                logger.debug(f'Finished processing record #{record_index}.\n')
        except json.decoder.JSONDecodeError:
        # except (requests.exceptions.JSONDecodeError,
        #         json.decoder.JSONDecodeError):
            logger.exception(f'{api_response_error_msg}: Error decoding JSON')
            logger.exception(f'{api_response.text=}')

            # Re-raise exception so that the script is halted (since future API
            # requests may result in the same error)
            raise

    def process_records(self, results: Dict[str, int]) -> None:
        """Checks each record in oclc_num_dict for the current OCLC number.

        Parameters
        ----------
        results: Dict[str, int]
            A dictionary containing the total number of records in the following
            categories: records with the current OCLC number, records with an
            old OCLC number, records with errors
        """

        logger.debug('Started processing records buffer...')

        timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        transactionID = f"DVP_{timestamp}_{os.getenv('WORLDCAT_PRINCIPAL_ID')}"
        url = (f"{os.getenv('WORLDCAT_METADATA_SERVICE_URL')}"
            f"/bib/checkcontrolnumbers"
            f"?oclcNumbers={','.join(self.oclc_num_dict.keys())}"
            f"&transactionID={transactionID}")
        headers = {"Accept": "application/json"}
        response = None

        try:
            response = self.oauth_session.get(url, headers=headers)
        except TokenExpiredError as e:
            logger.debug(f'Access token {self.oauth_session.access_token} '
                f'expired. Requesting new access token...')

            token = None
            if 'WORLDCAT_METADATA_API_REFRESH_TOKEN' in os.environ:
                # Use Refresh Token to request new Access Token
                token = self.oauth_session.refresh_token(
                    os.getenv('OCLC_AUTHORIZATION_SERVER_TOKEN_URL'),
                    refresh_token=os.getenv(
                        'WORLDCAT_METADATA_API_REFRESH_TOKEN'),
                    auth=self.auth)
            else:
                # Request Access Token
                token = self.oauth_session.fetch_token(
                    os.getenv('OCLC_AUTHORIZATION_SERVER_TOKEN_URL'),
                    auth=self.auth)
                logger.debug(f"Refresh token granted: {token['refresh_token']}")
                # Set Refresh Token environment variable and update .env file
                os.environ['WORLDCAT_METADATA_API_REFRESH_TOKEN'] = \
                    token['refresh_token']
                dotenv.set_key(
                    dotenv_file,
                    'WORLDCAT_METADATA_API_REFRESH_TOKEN',
                    os.environ['WORLDCAT_METADATA_API_REFRESH_TOKEN'])

            logger.debug(f'{token=}')

            dotenv_file_not_updated_msg = ('Note that .env file may not have '
                'been updated.')

            # Delete after testing assertion below
            logger.debug(f'Stored token was {os.getenv("WORLDCAT_METADATA_API_ACCESS_TOKEN")}')
            os.environ['WORLDCAT_METADATA_API_ACCESS_TOKEN'] = \
                token['access_token']
            logger.debug(f'Stored token is now {os.getenv("WORLDCAT_METADATA_API_ACCESS_TOKEN")}')

            assert token['access_token'] \
                != os.getenv('WORLDCAT_METADATA_API_ACCESS_TOKEN'), (
                f'Unable to retrieve new access token: Acquired token '
                f'{token["access_token"]} == stored token '
                f'{os.getenv("WORLDCAT_METADATA_API_ACCESS_TOKEN")}. '
                f'{dotenv_file_not_updated_msg}')

            logger.debug(f"New access token granted: {token['access_token']}")

            # Delete after testing assertion below
            token['access_token'] = 'hello'
            logger.debug(f'Changed access token to {token["access_token"]}')
            logger.debug(f'{self.oauth_session.access_token=}')

            assert token['access_token'] == self.oauth_session.access_token, (
                f"OAuth Session's access token "
                f"{self.oauth_session.access_token} was not updated to the "
                f"new access token {token['access_token']}. "
                f"{dotenv_file_not_updated_msg}")

            # Set environment variables based on new Access Token info and
            # update .env file accordingly
            os.environ['WORLDCAT_METADATA_API_ACCESS_TOKEN'] = \
                token['access_token']
            dotenv.set_key(
                dotenv_file,
                'WORLDCAT_METADATA_API_ACCESS_TOKEN',
                os.environ['WORLDCAT_METADATA_API_ACCESS_TOKEN'])

            os.environ['WORLDCAT_METADATA_API_ACCESS_TOKEN_TYPE'] = \
                token['token_type']
            dotenv.set_key(
                dotenv_file,
                'WORLDCAT_METADATA_API_ACCESS_TOKEN_TYPE',
                os.environ['WORLDCAT_METADATA_API_ACCESS_TOKEN_TYPE'])

            os.environ['WORLDCAT_METADATA_API_ACCESS_TOKEN_EXPIRES_AT'] = \
                str(token['expires_at'])
            dotenv.set_key(
                dotenv_file,
                'WORLDCAT_METADATA_API_ACCESS_TOKEN_EXPIRES_AT',
                os.environ['WORLDCAT_METADATA_API_ACCESS_TOKEN_EXPIRES_AT'])

            response = self.oauth_session.get(url, headers=headers)

        libraries.api.log_response_and_raise_for_status(response)
        self.process_api_response(response, results)
        logger.debug('Finished processing records buffer.\n')

    def remove_all_records(self) -> None:
        """Removes all records from this buffer (i.e. clears oclc_num_dict)."""

        self.oclc_num_dict.clear()
        logger.debug(f'Cleared records buffer.')
        logger.debug(self.__str__() + '\n')


def check_oclc_numbers(oclc_nums: str, oauth_session) -> requests.models.Response:
    """Checks each number in oclc_nums to see if it's the current one.

    This is done by sending a GET request to the WorldCat Metadata API:
    https://worldcat.org/bib/checkcontrolnumbers?oclcNumbers={oclcNumbers}

    Parameters
    ----------
    oclc_nums: str
        The OCLC numbers to be checked. Each OCLC number should be separated by
        a comma and contain no spaces, leading zeros, or non-digit characters.
    oauth_session:

    Returns
    -------
    requests.models.Response
        The API response object
    """

    # headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    # response = requests.get(f"{os.getenv('WORLDCAT_METADATA_SERVICE_URL')}"
    #     f"/bib/checkcontrolnumbers?oclcNumbers={oclc_nums}&transactionID="
    #     f"{transactionID}", headers=headers, timeout=45)

    libraries.api.log_response_and_raise_for_status(response)

    return response


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

    mms_ids_already_processed = set()
    logger.debug(f'{mms_ids_already_processed=}\n')

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

        records_with_errors_writer = writer(records_with_errors)

        records_buffer = RecordsBuffer(records_with_current_oclc_num,
            records_with_old_oclc_num, records_with_errors)
        logger.debug(f'{type(records_buffer)=}')
        logger.debug(records_buffer)
        logger.debug(f'{type(records_buffer.oclc_num_dict)=}\n')

        # Loop over each row in DataFrame and check whether OCLC number is the
        # current one
        for index, row in data.iterrows():
            logger.debug(f'Started processing row {index + 2} of input file...')
            error_occurred = False
            error_msg = None

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

                assert mms_id not in mms_ids_already_processed, ('MMS ID has '
                    'already been processed.')
                mms_ids_already_processed.add(mms_id)

                if len(records_buffer.oclc_num_dict) < int(os.getenv(
                        'WORLDCAT_METADATA_API_MAX_RECORDS_PER_REQUEST')):
                    records_buffer.add(orig_oclc_num, mms_id)
                else:
                    # records_buffer has the maximum records possible per API
                    # request, so process these records
                    logger.debug('Records buffer is full.\n')
                    records_buffer.process_records(results)

                    # Now that its records have been processed, clear buffer
                    records_buffer.remove_all_records()

                    # Add current row's data to the empty buffer
                    records_buffer.add(orig_oclc_num, mms_id)
            except AssertionError as assert_err:
                logger.exception(f"An assertion error occurred when "
                    f"processing MMS ID '{row['MMS ID']}' (at row {index + 2}"
                    f" of input file): {assert_err}")
                error_msg = f"Assertion Error: {assert_err}"
                error_occurred = True
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
        if len(records_buffer.oclc_num_dict) > 0:
            records_buffer.process_records(results)

    # logger.debug(f'{mms_ids_already_processed=}\n')
    logger.debug(f'{len(mms_ids_already_processed)=}\n')

    print(f'\nEnd of script. Processed {len(data.index)} rows from input file:'
        f'\n- {results["num_records_with_current_oclc_num"]} record(s) with '
        f'current OCLC number'
        f'\n- {results["num_records_with_old_oclc_num"]} record(s) with old '
        f'OCLC number'
        f'\n- {results["num_records_with_errors"]} record(s) with errors')


if __name__ == "__main__":
    main()
