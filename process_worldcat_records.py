import argparse
import calendar
import dotenv
import json
import libraries.api
import libraries.handle_file
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
from typing import Callable, Dict, Set, TextIO

dotenv_file = dotenv.find_dotenv()
dotenv.load_dotenv(dotenv_file)

logging.config.fileConfig('logging.conf', disable_existing_loggers=False)
logger = logging.getLogger(__name__)


class RecordsBuffer:
    """
    A buffer of records. DO NOT INSTANTIATE THIS CLASS DIRECTLY.

    Instead, instantiate one of its subclasses:
    - AlmaRecordsBuffer: A buffer of records with MMS ID and OCLC number
    - WorldCatRecordsBuffer: A buffer of records with OCLC number only

    Attributes
    ----------
    auth: HTTPBasicAuth
        The HTTP Basic Auth object used when requesting an access token
    oauth_session: OAuth2Session
        The OAuth 2 Session object used to request an access token and make HTTP
        requests to the WorldCat Metadata API (note that the OAuth2Session class
        is a subclass of requests.Session)

    Methods
    -------
    get_transaction_id()
        Builds transaction_id to include with WorldCat Metadata API request
    make_api_request(api_request, api_url)
        Makes the specified API request to the WorldCat Metadata API
    """

    def __init__(self) -> None:
        """Initializes a RecordsBuffer object by creating its OAuth2Session."""

        logger.debug('Started RecordsBuffer constructor...')
        self.contents = None
        logger.debug(f'{type(self.contents)=}')

        # Create OAuth2Session for WorldCat Metadata API
        logger.debug('Creating OAuth2Session...')

        self.auth = HTTPBasicAuth(os.environ['WORLDCAT_METADATA_API_KEY'],
            os.environ['WORLDCAT_METADATA_API_SECRET'])
        logger.debug(f'{type(self.auth)=}')

        client = BackendApplicationClient(
            client_id=os.environ['WORLDCAT_METADATA_API_KEY'],
            scope=['WorldCatMetadataAPI refresh_token'])
        token = {
            'access_token': os.environ['WORLDCAT_METADATA_API_ACCESS_TOKEN'],
            'expires_at': float(
                os.environ['WORLDCAT_METADATA_API_ACCESS_TOKEN_EXPIRES_AT']),
            'token_type': os.environ['WORLDCAT_METADATA_API_ACCESS_TOKEN_TYPE']
            }

        self.oauth_session = OAuth2Session(client=client, token=token)
        logger.debug(f'{type(self.oauth_session)=}')
        logger.debug('OAuth2Session created.')
        logger.debug('Completed RecordsBuffer constructor.')

    def __len__(self) -> int:
        """Returns the number of records in this records buffer.

        Returns
        -------
        int
            The number of records in this records buffer

        Raises
        ------
        TypeError
            If the contents attribute is not defined (i.e. is None)
        """

        return len(self.contents)

    def get_transaction_id(self) -> str:
        """Builds transaction_id to include with WorldCat Metadata API request.

        Returns
        -------
        str
            The transaction_id
        """

        transaction_id = ''
        if ('OCLC_INSTITUTION_SYMBOL' in os.environ
                or 'WORLDCAT_PRINCIPAL_ID' in os.environ):
            # Add OCLC Institution Symbol, if present
            transaction_id = os.getenv('OCLC_INSTITUTION_SYMBOL', '')

            if transaction_id != '':
                transaction_id += '_'

            # Add timestamp and, if present, your WorldCat Principal ID
            transaction_id += time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())

            if 'WORLDCAT_PRINCIPAL_ID' in os.environ:
                transaction_id += f"_{os.getenv('WORLDCAT_PRINCIPAL_ID')}"

        logger.debug(f'{transaction_id=}')

        return transaction_id

    def make_api_request(
            self,
            api_request: Callable[..., requests.models.Response],
            api_url: str) -> requests.models.Response:
        """Makes the specified API request to the WorldCat Metadata API.

        Parameters
        ----------
        api_request: Callable[..., requests.models.Response]
            The specific WorldCat Metadata API request to make
        api_url: str
            The specific WorldCat Metadata API URL to use

        Returns
        -------
        requests.models.Response
            The API response returned by the api_request function
        """

        transaction_id = self.get_transaction_id()

        if transaction_id != '':
            api_url += f"&transactionID={transaction_id}"

        headers = {"Accept": "application/json"}
        response = None

        # Make API request
        try:
            response = api_request(api_url, headers=headers)
        except TokenExpiredError as e:
            logger.debug(f'Access token {self.oauth_session.access_token} '
                f'expired. Requesting new access token...')

            datetime_format = '%Y-%m-%d %H:%M:%SZ'

            # Confirm the epoch is January 1, 1970, 00:00:00 (UTC).
            # See https://docs.python.org/3.8/library/time.html for an
            # explanation of the term 'epoch'.
            system_epoch = time.strftime(datetime_format, time.gmtime(0))
            expected_epoch = '1970-01-01 00:00:00Z'
            if system_epoch != expected_epoch:
                logger.warning(f"The system's epoch ({system_epoch}) is not "
                    f"equal to the expected epoch ({expected_epoch}). There "
                    f"may therefore be issues in determining whether the "
                    f"WorldCat Metadata API's refresh token has expired.")

            # Convert the WORLDCAT_METADATA_API_REFRESH_TOKEN_EXPIRES_AT value
            # to a float representing seconds since the epoch.
            # Note that the WORLDCAT_METADATA_API_REFRESH_TOKEN_EXPIRES_AT value
            # is a string in ISO 8601 format, except that it substitutes the 'T'
            # delimiter (which separates the date from the time) for a space, as
            # in '2021-09-30 22:43:07Z'.
            refresh_token_expires_at = 0.0
            if 'WORLDCAT_METADATA_API_REFRESH_TOKEN_EXPIRES_AT' in os.environ:
                logger.debug(f'WORLDCAT_METADATA_API_REFRESH_TOKEN_EXPIRES_AT '
                    f'variable exists in .env file, so using this value: '
                    f'{os.getenv("WORLDCAT_METADATA_API_REFRESH_TOKEN_EXPIRES_AT")}'
                    f' (UTC), which will be converted to seconds since the '
                    f'epoch')
                refresh_token_expires_at = calendar.timegm(
                    time.strptime(
                        os.getenv(
                            'WORLDCAT_METADATA_API_REFRESH_TOKEN_EXPIRES_AT'),
                        datetime_format))

            refresh_token_expires_in = refresh_token_expires_at - time.time()
            logger.debug(f'{refresh_token_expires_at=} seconds since the epoch')
            logger.debug(f'Current time: {time.time()} seconds since the epoch,'
                f' which is {time.strftime(datetime_format, time.gmtime())} '
                f'(UTC). So the Refresh Token (if one exists) expires in '
                f'{refresh_token_expires_in} seconds.')

            # Obtain a new Access Token
            token = None
            if ('WORLDCAT_METADATA_API_REFRESH_TOKEN' in os.environ
                    and refresh_token_expires_in > 25):
                # Use Refresh Token to request new Access Token
                token = self.oauth_session.refresh_token(
                    os.environ['OCLC_AUTHORIZATION_SERVER_TOKEN_URL'],
                    refresh_token=os.getenv(
                        'WORLDCAT_METADATA_API_REFRESH_TOKEN'),
                    auth=self.auth)
            else:
                # Request Refresh Token and Access Token
                token = self.oauth_session.fetch_token(
                    os.environ['OCLC_AUTHORIZATION_SERVER_TOKEN_URL'],
                    auth=self.auth)
                logger.debug(f"Refresh token granted ({token['refresh_token']})"
                    f", which expires at {token['refresh_token_expires_at']}")

                # Set Refresh Token environment variables and update .env file
                libraries.handle_file.set_env_var(
                    'WORLDCAT_METADATA_API_REFRESH_TOKEN',
                    token['refresh_token'])

                libraries.handle_file.set_env_var(
                    'WORLDCAT_METADATA_API_REFRESH_TOKEN_EXPIRES_AT',
                    token['refresh_token_expires_at'])

            logger.debug(f'{token=}')
            logger.debug(f'New access token granted: '
                f'{self.oauth_session.access_token}')

            # Set environment variables based on new Access Token info and
            # update .env file accordingly
            libraries.handle_file.set_env_var(
                'WORLDCAT_METADATA_API_ACCESS_TOKEN',
                token['access_token'])

            libraries.handle_file.set_env_var(
                'WORLDCAT_METADATA_API_ACCESS_TOKEN_TYPE',
                token['token_type'])

            logger.debug(f"{token['expires_at']=}")

            libraries.handle_file.set_env_var(
                'WORLDCAT_METADATA_API_ACCESS_TOKEN_EXPIRES_AT',
                str(token['expires_at']))

            response = api_request(api_url, headers=headers)

        libraries.api.log_response_and_raise_for_status(response)
        return response


class AlmaRecordsBuffer(RecordsBuffer):
    """
    A buffer of Alma records, each with an MMS ID and OCLC number.

    Attributes
    ----------
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
    process_records(results)
        Checks each record in oclc_num_dict for the current OCLC number
    remove_all_records()
        Removes all records from this buffer (i.e. clears oclc_num_dict)
    """

    def __init__(self,
            records_with_current_oclc_num: TextIO,
            records_with_old_oclc_num: TextIO,
            records_with_errors: TextIO) -> None:
        """Instantiates an AlmaRecordsBuffer object.

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

        logger.debug('Started AlmaRecordsBuffer constructor...')

        self.oclc_num_dict = {}
        logger.debug(f'{type(self.oclc_num_dict)=}')

        self.records_with_current_oclc_num = records_with_current_oclc_num
        self.records_with_current_oclc_num_writer = \
            writer(records_with_current_oclc_num)

        self.records_with_old_oclc_num = records_with_old_oclc_num
        self.records_with_old_oclc_num_writer = \
            writer(records_with_old_oclc_num)

        self.records_with_errors = records_with_errors
        self.records_with_errors_writer = writer(records_with_errors)

        # Create OAuth2Session for WorldCat Metadata API
        super().__init__()

        self.contents = self.oclc_num_dict
        logger.debug(f'{type(self.contents)=}')

        logger.debug('Completed AlmaRecordsBuffer constructor.\n')

    def __str__(self) -> str:
        """Returns a string listing the contents of this records buffer.

        In specific, this method lists the contents of the OCLC Number
        dictionary.

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

    def process_records(self, results: Dict[str, int]) -> None:
        """Checks each record in oclc_num_dict for the current OCLC number.

        This is done by making a GET request to the WorldCat Metadata API:
        https://worldcat.org/bib/checkcontrolnumbers?oclcNumbers={oclcNumbers}

        Parameters
        ----------
        results: Dict[str, int]
            A dictionary containing the total number of records in the following
            categories: records with the current OCLC number, records with an
            old OCLC number, records with errors

        Raises
        ------
        json.decoder.JSONDecodeError
            If there is an error decoding the API response
        """

        logger.debug('Started processing records buffer...')

        api_response_error_msg = ('Problem with Get Current OCLC Number API '
            'response')

        # Build URL for API request
        url = (f"{os.environ['WORLDCAT_METADATA_SERVICE_URL']}"
            f"/bib/checkcontrolnumbers"
            f"?oclcNumbers={','.join(self.oclc_num_dict.keys())}")

        try:
            api_response = super().make_api_request(
                self.oauth_session.get,
                url
            )
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

                    # Add record to
                    # records_with_errors_when_getting_current_oclc_number.csv
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

        logger.debug('Finished processing records buffer.')

    def remove_all_records(self) -> None:
        """Removes all records from this buffer (i.e. clears oclc_num_dict)."""

        self.oclc_num_dict.clear()
        logger.debug(f'Cleared records buffer.')
        logger.debug(self.__str__() + '\n')


class WorldCatRecordsBuffer(RecordsBuffer):
    """
    A buffer of WorldCat records, each with an OCLC number.

    Attributes
    ----------
    oclc_num_set: Set[str]
        A set containing each record's OCLC number
    records_with_holding_already_set: TextIO
        The CSV file object where records whose holding is already set are added
        (i.e. records that were not updated)
    records_with_holding_already_set_writer: writer
        The CSV writer object for the records_with_holding_already_set file
        object
    records_with_holding_successfully_set: TextIO
        The CSV file object where records whose holding was successfully set are
        added (i.e. records that were successfully updated)
    records_with_holding_successfully_set_writer: writer
        The CSV writer object for the records_with_holding_successfully_set file
        object
    records_with_errors: TextIO
        The CSV file object where records are added if an error is encountered
    records_with_errors_writer: writer
        The CSV writer object for the records_with_errors file object

    Methods
    -------
    add(oclc_num)
        Adds the given record to this buffer (i.e. to oclc_num_set)
    process_records(results)
        Attempts to set the institution holding for each record in oclc_num_set
    remove_all_records()
        Removes all records from this buffer (i.e. clears oclc_num_set)
    """

    def __init__(self,
            records_with_holding_already_set: TextIO,
            records_with_holding_successfully_set: TextIO,
            records_with_errors: TextIO) -> None:
        """Instantiates a WorldCatRecordsBuffer object.

        Parameters
        ----------
        records_with_holding_already_set: TextIO
            The CSV file object where records whose holding is already set are
            added (i.e. records that were not updated)
        records_with_holding_successfully_set: TextIO
            The CSV file object where records whose holding was successfully set
            are added (i.e. records that were successfully updated)
        records_with_errors: TextIO
            The CSV file object where records are added if an error is
            encountered
        """

        logger.debug('Started WorldCatRecordsBuffer constructor...')

        self.oclc_num_set = set()
        logger.debug(f'{type(self.oclc_num_set)=}')

        self.records_with_holding_already_set = records_with_holding_already_set
        self.records_with_holding_already_set_writer = \
            writer(records_with_holding_already_set)

        self.records_with_holding_successfully_set = \
            records_with_holding_successfully_set
        self.records_with_holding_successfully_set_writer = \
            writer(records_with_holding_successfully_set)

        self.records_with_errors = records_with_errors
        self.records_with_errors_writer = writer(records_with_errors)

        # Create OAuth2Session for WorldCat Metadata API
        super().__init__()

        self.contents = self.oclc_num_set
        logger.debug(f'{type(self.contents)=}')

        logger.debug('Completed WorldCatRecordsBuffer constructor.\n')

    def __str__(self) -> str:
        """Returns a string listing the contents of this records buffer.

        In specific, this method lists the contents of the OCLC Number set.

        Returns
        -------
        str
            The contents of the OCLC Number set
        """

        return (f'Records buffer contents (OCLC Numbers): {self.oclc_num_set}')

    def add(self, oclc_num: str) -> None:
        """Adds the given record to this buffer (i.e. to oclc_num_set).

        Parameters
        ----------
        oclc_num: str
            The record's OCLC number

        Raises
        ------
        AssertionError
            If the OCLC number is already in the OCLC Number set
        """

        assert oclc_num not in self.oclc_num_set, (f'OCLC number {oclc_num} '
            f'already exists in records buffer')
        self.oclc_num_set.add(oclc_num)
        logger.debug(f'Added {oclc_num} to records buffer.')

    def process_records(self, results: Dict[str, int]) -> None:
        """Attempts to set the holding for each record in oclc_num_set.

        This is done by making a POST request to the WorldCat Metadata API:
        https://worldcat.org/ih/datalist?oclcNumbers={oclcNumbers}

        Parameters
        ----------
        results: Dict[str, int]
            A dictionary containing the total number of records in the following
            categories: records successfully set, records already set, records
            with errors

        Raises
        ------
        json.decoder.JSONDecodeError
            If there is an error decoding the API response
        """

        logger.debug('Started processing records buffer...')

        api_response_error_msg = ('Problem with Set Holding API response')

        # Build URL for API request
        url = (f"{os.environ['WORLDCAT_METADATA_SERVICE_URL']}"
            f"/ih/datalist?oclcNumbers={','.join(self.oclc_num_set)}")

        try:
            api_response = super().make_api_request(
                self.oauth_session.post,
                url
            )
            json_response = api_response.json()
            logger.debug(f'Set Holding API response:\n'
                f'{json.dumps(json_response, indent=2)}')

            for record_index, record in enumerate(json_response['entry'],
                    start=1):
                is_current_oclc_num = (record['requestedOclcNumber']
                    == record['currentOclcNumber'])

                new_oclc_num = ''
                oclc_num_msg = ''
                if not is_current_oclc_num:
                    new_oclc_num = record['currentOclcNumber']
                    oclc_num_msg = (f'OCLC number '
                        f'{record["requestedOclcNumber"]} has been updated to '
                        f'{new_oclc_num}. Consider updating Alma record.')
                    logger.warning(oclc_num_msg)
                    oclc_num_msg = f'Warning: {oclc_num_msg}'

                logger.debug(f'Started processing record #{record_index} (OCLC '
                    f'number {record["requestedOclcNumber"]})...')
                logger.debug(f'{is_current_oclc_num=}')
                logger.debug(f'{record["httpStatusCode"]=}')
                logger.debug(f'{record["errorDetail"]=}')

                if record['httpStatusCode'] == 'HTTP 200 OK':
                    results['num_records_successfully_set'] += 1

                    # Add record to records_with_holding_successfully_set.csv
                    if self.records_with_holding_successfully_set.tell() == 0:
                        # Write header row
                        self.records_with_holding_successfully_set_writer.writerow([
                            'Requested OCLC Number',
                            'New OCLC Number (if applicable)',
                            'Warning'
                        ])

                    self.records_with_holding_successfully_set_writer.writerow([
                        record['requestedOclcNumber'],
                        new_oclc_num,
                        oclc_num_msg
                    ])
                elif record['httpStatusCode'] == 'HTTP 409 Conflict':
                    results['num_records_already_set'] += 1

                    # Add record to records_with_holding_already_set.csv
                    if self.records_with_holding_already_set.tell() == 0:
                        # Write header row
                        self.records_with_holding_already_set_writer.writerow([
                            'Requested OCLC Number',
                            'New OCLC Number (if applicable)',
                            'Error'
                        ])

                    self.records_with_holding_already_set_writer.writerow([
                        record['requestedOclcNumber'],
                        new_oclc_num,
                        (f"{api_response_error_msg}: {record['errorDetail']}. "
                            f"{oclc_num_msg}")
                    ])
                else:
                    logger.exception(f"{api_response_error_msg} for OCLC "
                        f"Number {record['requestedOclcNumber']}: "
                        f"{record['errorDetail']} ({record['httpStatusCode']})."
                    )

                    results['num_records_with_errors'] += 1

                    # Add record to records_with_errors_when_setting_holding.csv
                    if self.records_with_errors.tell() == 0:
                        # Write header row
                        self.records_with_errors_writer.writerow([
                            'Requested OCLC Number',
                            'New OCLC Number (if applicable)',
                            'Error'
                        ])

                    self.records_with_errors_writer.writerow([
                        record['requestedOclcNumber'],
                        new_oclc_num,
                        (f"{api_response_error_msg}: {record['httpStatusCode']}"
                            f": {record['errorDetail']}. {oclc_num_msg}")
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

        logger.debug('Finished processing records buffer.')

    def remove_all_records(self) -> None:
        """Removes all records from this buffer (i.e. clears oclc_num_set)."""

        self.oclc_num_set.clear()
        logger.debug(f'Cleared records buffer.')
        logger.debug(self.__str__() + '\n')


def init_argparse() -> argparse.ArgumentParser:
    """Initializes and returns ArgumentParser object."""

    parser = argparse.ArgumentParser(
        usage=('%(prog)s [-h] [-v] --input_file INPUT_FILE --operation '
            '{get_current_oclc_number, set_holding}'),
        description=('For each row in the input file, perform the specified '
            'operation.')
    )
    parser.add_argument(
        '-v', '--version', action='version',
        version=f'{parser.prog} version 1.0.0'
    )
    parser.add_argument(
        '--input_file',
        required=True,
        type=str,
        help=('the name and path of the file to be processed, which must be in '
            'CSV format (e.g. '
            'csv/master_list_records_with_potentially_old_oclc_num.csv)')
    )
    parser.add_argument(
        '--operation',
        required=True,
        choices=['get_current_oclc_number', 'set_holding'],
        help=('the operation to be performed on each row of the input file '
            '(either get_current_oclc_number or set_holding)')
    )
    return parser


def main() -> None:
    """Performs the specified operation on every record in the input file.

    Gathers the maximum OCLC numbers possible before sending the appropriate
    request to the WorldCat Metadata API.

    Operations:
    - get_current_oclc_number
        For each row, check whether the given OCLC number is the current one:
        -- If so, then add the record to csv/already_has_current_oclc_number.csv
        -- If not, then add the record to csv/needs_current_oclc_number.csv
        -- If an error is encountered, then add the record to
           csv/records_with_errors_when_getting_current_oclc_number.csv

    - set_holding
        For each row, set holding for the given OCLC number
        -- If holding is set successfully, then add the record to
           csv/records_with_holding_successfully_set.csv
        -- If holding was already set, then add the record to
           csv/records_with_holding_already_set.csv
        -- If an error is encountered, then add the record to
           csv/records_with_errors_when_setting_holding.csv
    """

    # Initialize parser and parse command-line args
    parser = init_argparse()
    args = parser.parse_args()

    # Convert input file into pandas DataFrame
    data = None
    if args.input_file.endswith('.csv'):
        data = pd.read_csv(args.input_file, dtype='str', keep_default_na=False)
    else:
        logger.exception(f'Invalid format for input file ({args.input_file}). '
            f'Must be a CSV file (.csv)')
        return

    records_already_processed = set()
    logger.debug(f'{records_already_processed=}\n')

    logger.debug(f'{args.operation=}')

    results = None
    filename_for_records_to_update = None
    filename_for_records_with_no_update_needed = None
    filename_for_records_with_errors = None

    if args.operation == 'get_current_oclc_number':
        results = {
            'num_records_with_current_oclc_num': 0,
            'num_records_with_old_oclc_num': 0,
            'num_records_with_errors': 0
        }
        filename_for_records_to_update = 'csv/needs_current_oclc_number.csv'
        filename_for_records_with_no_update_needed = \
            'csv/already_has_current_oclc_number.csv'
        filename_for_records_with_errors = \
            'csv/records_with_errors_when_getting_current_oclc_number.csv'
    else:
        results = {
            'num_records_successfully_set': 0,
            'num_records_already_set': 0,
            'num_records_with_errors': 0
        }
        filename_for_records_to_update = \
            'csv/records_with_holding_successfully_set.csv'
        filename_for_records_with_no_update_needed = \
            'csv/records_with_holding_already_set.csv'
        filename_for_records_with_errors = \
            'csv/records_with_errors_when_setting_holding.csv'

    with open(filename_for_records_to_update, mode='a',
            newline='') as records_to_update, \
        open(filename_for_records_with_no_update_needed, mode='a',
            newline='') as records_with_no_update_needed, \
        open(filename_for_records_with_errors, mode='a',
            newline='') as records_with_errors:

        records_with_errors_writer = writer(records_with_errors)

        records_buffer = None
        if args.operation == 'get_current_oclc_number':
            records_buffer = AlmaRecordsBuffer(
                records_with_no_update_needed,
                records_to_update,
                records_with_errors
            )
        else:
            records_buffer = WorldCatRecordsBuffer(
                records_with_no_update_needed,
                records_to_update,
                records_with_errors
            )

        logger.debug(f'{type(records_buffer)=}')
        logger.debug(records_buffer)
        logger.debug(f'{type(records_buffer.contents)=}')
        logger.debug(f'{len(records_buffer)=}\n')

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

    # logger.debug(f'{records_already_processed=}\n')
    logger.debug(f'{len(records_already_processed)=}\n')

    print(f'\nEnd of script. Processed {len(data.index)} rows from input file:')

    if args.operation == 'get_current_oclc_number':
        print(f'- {results["num_records_with_current_oclc_num"]} record(s) '
            f'with current OCLC number\n'
            f'- {results["num_records_with_old_oclc_num"]} record(s) with '
            f'old OCLC number\n'
            f'- {results["num_records_with_errors"]} record(s) with errors')
    else:
        print(f'- {results["num_records_successfully_set"]} record(s) updated, '
            f'i.e. holding was successfully set\n'
            f'- {results["num_records_already_set"]} record(s) not updated '
            f'because holding was already set\n'
            f'- {results["num_records_with_errors"]} record(s) with errors')

if __name__ == "__main__":
    main()
