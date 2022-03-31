import calendar
import dotenv
import json
import libraries.api
import libraries.handle_file
import libraries.record
import logging
import os
import pandas as pd
import requests
import time
from csv import writer
from oauthlib.oauth2 import BackendApplicationClient, TokenExpiredError
from requests.auth import HTTPBasicAuth
from requests_oauthlib import OAuth2Session
from typing import (Any, Callable, Dict, List, NamedTuple, Optional, Set,
    TextIO, Tuple, Union)

dotenv_file = dotenv.find_dotenv()
dotenv.load_dotenv(dotenv_file)

logger = logging.getLogger(__name__)


class RecordsBuffer:
    """
    A buffer of records. DO NOT INSTANTIATE THIS CLASS DIRECTLY.

    Instead, instantiate one of its subclasses:
    - AlmaRecordsBuffer: A buffer of records with MMS ID and OCLC number
    - WorldCatRecordsBuffer: A buffer of records with OCLC number only
    - WorldCatSearchBuffer: A buffer containing data to be searched for in
      WorldCat (use this subclass to find a record's OCLC Number given other
      record identifiers; see search_worldcat.py for more details)

    Attributes
    ----------
    auth: HTTPBasicAuth
        The HTTP Basic Auth object used when requesting an access token
    contents: Union[Dict[str, str], Set[str], List[NamedTuple]]
        The contents of the buffer (this attribute is defined in the subclass)
    num_api_requests_made: int
        The total number of WorldCat Metadata API requests made using this
        records buffer
    oauth_session: OAuth2Session
        The OAuth2Session object used to request an access token and make HTTP
        requests to the WorldCat Metadata API (note that the OAuth2Session class
        is a subclass of requests.Session)

    Methods
    -------
    get_transaction_id()
        Builds the transaction_id to include with the WorldCat Metadata API
        request
    make_api_request(api_request, api_url)
        Makes the specified API request to the WorldCat Metadata API
    """

    def __init__(self) -> None:
        """Initializes a RecordsBuffer object by creating its OAuth2Session."""

        self.contents = None
        self.num_api_requests_made = 0

        # Create OAuth2Session for WorldCat Metadata API
        self.auth = HTTPBasicAuth(os.environ['WORLDCAT_METADATA_API_KEY'],
            os.environ['WORLDCAT_METADATA_API_SECRET'])

        client = BackendApplicationClient(
            client_id=os.environ['WORLDCAT_METADATA_API_KEY'],
            scope=['WorldCatMetadataAPI refresh_token'])

        token = {
            'access_token': os.environ['WORLDCAT_METADATA_API_ACCESS_TOKEN'],
            'expires_at': float(
                os.environ['WORLDCAT_METADATA_API_ACCESS_TOKEN_EXPIRES_AT']),
            'token_type': os.environ['WORLDCAT_METADATA_API_ACCESS_TOKEN_TYPE']}

        self.oauth_session = OAuth2Session(client=client, token=token)

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

            libraries.handle_file.set_env_var(
                'WORLDCAT_METADATA_API_ACCESS_TOKEN_EXPIRES_AT',
                str(token['expires_at']))

            response = api_request(api_url, headers=headers)

        self.num_api_requests_made += 1
        libraries.api.log_response_and_raise_for_status(response)
        return response

    def search_worldcat_and_log_response(
            self,
            api_request: Callable[..., requests.models.Response],
            api_url: str,
            api_response_label: str
        ) -> Tuple[requests.models.Response, Dict[str, Any]]:
        """Makes the specified API request and logs the response.

        Parameters
        ----------
        api_request: Callable[..., requests.models.Response]
            The specific WorldCat Metadata API request to make
        api_url: str
            The specific WorldCat Metadata API URL to use
        api_response_label: str
            The label (which identifies the API response) to use when logging
            the response

        Returns
        -------
        Tuple[requests.models.Response, Dict[str, Any]]
            Tuple with the API response and its corresponding JSON response
        """

        api_response = self.make_api_request(
            api_request,
            api_url)

        json_response = api_response.json()

        logger.debug(f'{api_response_label}:\n'
            f'{json.dumps(json_response, indent=2)}')

        return api_response, json_response


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

        self.oclc_num_dict = {}

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

        This is done by making a GET request to the WorldCat Metadata API v1.0:
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
        url = (f"{os.environ['WORLDCAT_METADATA_API_URL']}"
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
                    logger.error(f'{api_response_error_msg}: OCLC number '
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
    set_or_unset_choice: str
        The operation to perform on each WorldCat record in this buffer (i.e.
        either 'set' or 'unset' holding)
    cascade: str
        Only applicable to the unset_holding operation: whether or not to
        execute the operation if a local holdings record or local bibliographic
        record exists:
        0 - don't unset holding if local holdings record or local bibliographic
            records exists;
        1 - unset holding and delete local holdings record and local
            bibliographic record (if one exists)
    records_with_no_update_needed: TextIO
        The CSV file object where records whose holding was already set or unset
        are added (i.e. records that did not need to be updated)
    records_with_no_update_needed_writer: writer
        The CSV writer object for the records_with_no_update_needed file object
    records_updated: TextIO
        The CSV file object where records whose holding was successfully set or
        unset are added (i.e. records that were successfully updated)
    records_updated_writer: writer
        The CSV writer object for the records_updated file object
    records_with_errors: TextIO
        The CSV file object where records are added if an error is encountered
    records_with_errors_writer: writer
        The CSV writer object for the records_with_errors file object

    Methods
    -------
    add(oclc_num)
        Adds the given record to this buffer (i.e. to oclc_num_set)
    process_records(results)
        Attempts to set or unset the holding for each record in oclc_num_set
    remove_all_records()
        Removes all records from this buffer (i.e. clears oclc_num_set)
    """

    def __init__(self,
            set_or_unset_choice: str,
            cascade: str,
            records_with_no_update_needed: TextIO,
            records_updated: TextIO,
            records_with_errors: TextIO) -> None:
        """Instantiates a WorldCatRecordsBuffer object.

        Parameters
        ----------
        set_or_unset_choice: str
            The operation to perform on each WorldCat record in this buffer
            (i.e. either 'set' or 'unset' holding)
        cascade: str
            Only applicable to the unset_holding operation: whether or not to
            execute the operation if a local holdings record or local
            bibliographic record exists:
            0 - don't unset holding if local holdings record or local
                bibliographic records exists;
            1 - unset holding and delete local holdings record and local
                bibliographic record (if one exists)
        records_with_no_update_needed: TextIO
            The CSV file object where records whose holding was already set or
            unset are added (i.e. records that did not need to be updated)
        records_updated: TextIO
            The CSV file object where records whose holding was successfully set
            or unset are added (i.e. records that were successfully updated)
        records_with_errors: TextIO
            The CSV file object where records are added if an error is
            encountered
        """

        self.oclc_num_set = set()

        self.set_or_unset_choice = set_or_unset_choice
        logger.debug(f'{self.set_or_unset_choice=}\n')

        self.cascade = cascade
        if self.set_or_unset_choice == 'unset':
            logger.debug(f'{self.cascade=}\n')

        self.records_with_no_update_needed = records_with_no_update_needed
        self.records_with_no_update_needed_writer = \
            writer(records_with_no_update_needed)

        self.records_updated = records_updated
        self.records_updated_writer = writer(records_updated)

        self.records_with_errors = records_with_errors
        self.records_with_errors_writer = writer(records_with_errors)

        # Create OAuth2Session for WorldCat Metadata API
        super().__init__()

        self.contents = self.oclc_num_set

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
        """Attempts to set or unset the holding for each record in oclc_num_set.

        This is done by making a POST request (if setting holdings) or a DELETE
        request (if unsetting holdings) to the WorldCat Metadata API v1.0:
        https://worldcat.org/ih/datalist?oclcNumbers={oclcNumbers}

        If unsetting holdings, the "cascade" URL parameter is also included.
        For example:
        https://worldcat.org/ih/datalist?oclcNumbers={oclcNumbers}&cascade=0

        Parameters
        ----------
        results: Dict[str, int]
            A dictionary containing the total number of records in the following
            categories: records updated, records with no update needed, records
            with errors

        Raises
        ------
        json.decoder.JSONDecodeError
            If there is an error decoding the API response
        """

        logger.debug('Started processing records buffer...')

        api_name = None
        if self.set_or_unset_choice == 'set':
            api_name = 'Set Holding API'
        else:
            api_name = 'Unset Holding API'

        api_response_error_msg = f'Problem with {api_name} response'

        # Build URL for API request
        url = (f"{os.environ['WORLDCAT_METADATA_API_URL']}"
            f"/ih/datalist?oclcNumbers={','.join(self.oclc_num_set)}")

        try:
            api_response = None
            if self.set_or_unset_choice == 'set':
                api_response = super().make_api_request(
                    self.oauth_session.post,
                    url
                )
            else:
                # Include "cascade" URL parameter for unset_holding operation
                url += f'&cascade={self.cascade}'
                api_response = super().make_api_request(
                    self.oauth_session.delete,
                    url
                )
            json_response = api_response.json()
            logger.debug(f'{api_name} response:\n'
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
                    results['num_records_updated'] += 1

                    # Add record to
                    # records_with_holding_successfully_{set_or_unset_choice}.csv
                    if self.records_updated.tell() == 0:
                        # Write header row
                        self.records_updated_writer.writerow([
                            'Requested OCLC Number',
                            'New OCLC Number (if applicable)',
                            'Warning'
                        ])

                    self.records_updated_writer.writerow([
                        record['requestedOclcNumber'],
                        new_oclc_num,
                        oclc_num_msg
                    ])
                elif record['httpStatusCode'] == 'HTTP 409 Conflict':
                    results['num_records_with_no_update_needed'] += 1

                    # Add record to
                    # records_with_holding_already_{set_or_unset_choice}.csv
                    if self.records_with_no_update_needed.tell() == 0:
                        # Write header row
                        self.records_with_no_update_needed_writer.writerow([
                            'Requested OCLC Number',
                            'New OCLC Number (if applicable)',
                            'Error'
                        ])

                    self.records_with_no_update_needed_writer.writerow([
                        record['requestedOclcNumber'],
                        new_oclc_num,
                        (f"{api_response_error_msg}: {record['errorDetail']}. "
                            f"{oclc_num_msg}")
                    ])
                else:
                    logger.error(f"{api_response_error_msg} for OCLC "
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


class WorldCatSearchBuffer(RecordsBuffer):
    """
    A buffer containing data to be searched for in WorldCat.

    This buffer must contain only one record at a time.

    Attributes
    ----------
    dataframe_for_input_file: pd.DataFrame
        The pandas DataFrame created from the input file
    record_list: List[NamedTuple]
        A list containing the record data to use when searching WorldCat; this
        list should contain no more than one element (i.e. record)

    Methods
    -------
    add(record_data)
        Adds the given record to this buffer (i.e. to record_list)
    process_records()
        Searches WorldCat using the record data in record_list
    remove_all_records()
        Removes all records from this buffer (i.e. clears record_list)
    """

    def __init__(self, dataframe_for_input_file: pd.DataFrame) -> None:
        """Instantiates a WorldCatSearchBuffer object.

        Parameters
        ----------
        dataframe_for_input_file: pd.DataFrame
            The pandas DataFrame created from the input file
        """

        self.record_list = []
        self.dataframe_for_input_file = dataframe_for_input_file

        # Create OAuth2Session for WorldCat Metadata API
        super().__init__()

        self.contents = self.record_list

    def __str__(self) -> str:
        """Returns a string listing the contents of this records buffer.

        In specific, this method lists the contents of the record_list.

        Returns
        -------
        str
            The contents of the record_list
        """

        return (f'Records buffer contents: {self.record_list}')

    def add(self, record_data: NamedTuple) -> None:
        """Adds the given record to this buffer (i.e. to record_list).

        Parameters
        ----------
        record_data: NamedTuple
            The record data to use when searching WorldCat

        Raises
        ------
        AssertionError
            If adding to a non-empty record_list (this list should never contain
            more than one record)
        """

        assert super().__len__() == 0, (f'Cannot add to a non-empty '
            f'WorldCatSearchBuffer. Buffer currently contains '
            f'{super().__len__()} record(s).')
        self.record_list.append(record_data)
        logger.debug(f'Added {record_data} to records buffer.')

    def get_num_records_dict(
            self,
            num_records: int,
            records_label: Optional[str] = None) -> Dict[str, Union[int, str]]:
        """Creates a dictionary with data about the WorldCat search results.

        Parameters
        ----------
        num_records: int
            The number of records returned by the WorldCat search
        records_label: Optional[str]
            String describing the records (e.g. that they are held by your
            library)

        Returns
        -------
        Dict[str, Union[int, str]]
            The dictionary created based on the given WorldCat search data
        """

        column_name = None
        log_msg = None

        if records_label is None:
            column_name = "num_records_total"
            log_msg = f"found {num_records} total records"
        else:
            column_name = (f"num_records_held_by_"
                f"{os.environ['OCLC_INSTITUTION_SYMBOL']}")
            log_msg = f"found {num_records} {records_label}"

        return {
            'value': num_records,
            'column_name': column_name,
            'log_msg': log_msg
        }

    def process_records(self) -> None:
        """Searches WorldCat using the record data in record_list.

        The WorldCat search is performed using the first available record
        identifier (in this order):
        - lccn_fixed (i.e. a corrected version of the lccn value)
        - lccn
        - isbn (accepts multiple values separated by a semicolon)
        - issn (accepts multiple values separated by a semicolon)
        - gov_doc_class_num_086 (i.e. MARC field 086: Government Document
          Classification Number): If the gpo_item_num_074 (i.e. MARC field 074:
          GPO Item Number) is also available, then a combined search is
          performed (gov_doc_class_num_086 AND gpo_item_num_074). If only
          gpo_item_num_074 is available, then no search is performed.

        This is done by making a GET request to the WorldCat Metadata API v1.1:
        https://americas.metadata.api.oclc.org/worldcat/search/v1/brief-bibs?q={search_query}

        Raises
        ------
        AssertionError
            If buffer (i.e. record_list) does not contain exactly one record OR
            if a valid search query cannot be built (because all record
            identifiers are either empty or invalid)
        json.decoder.JSONDecodeError
            If there is an error decoding the API response
        """

        logger.debug('Started processing records buffer...')

        assert super().__len__() == 1, (f'Buffer must contain exactly one '
            f'record but instead contains {super().__len__()} records. Cannot '
            f'process buffer.')

        # Build search query
        search_query = None
        if (hasattr(self.record_list[0], 'lccn_fixed')
                and (lccn_fixed := self.record_list[0].lccn_fixed.strip())
                    != ''):
            search_query = f'nl:{lccn_fixed}'
        elif (hasattr(self.record_list[0], 'lccn')
                and (lccn := self.record_list[0].lccn.strip()) != ''):
            search_query = f'nl:{lccn}'
        elif (hasattr(self.record_list[0], 'isbn')
                and (isbn := libraries.record.split_and_join_record_identifiers(
                    self.record_list[0].isbn,
                    identifier_name='isbn',
                    split_separator=';')) != ''):
            search_query = f'bn:{isbn}'
        elif (hasattr(self.record_list[0], 'issn')
                and (issn := libraries.record.split_and_join_record_identifiers(
                    self.record_list[0].issn,
                    identifier_name='issn',
                    split_separator=';')) != ''):
            search_query = f'in:{issn}'
        elif hasattr(self.record_list[0], 'gov_doc_class_num_086'):
            gov_doc_class_num_086 = (
                libraries.record.split_and_join_record_identifiers(
                    self.record_list[0].gov_doc_class_num_086,
                    identifier_name='gov_doc_class_num_086',
                    split_separator=';',
                    join_separator=' OR '))
            if gov_doc_class_num_086 != '':
                search_query = gov_doc_class_num_086

                # If 074 field exists and has a nonempty value, then combine 086
                # and 074 values
                if hasattr(self.record_list[0], 'gpo_item_num_074'):
                    gpo_item_num_074 = (
                        libraries.record.split_and_join_record_identifiers(
                            self.record_list[0].gpo_item_num_074,
                            identifier_name='gpo_item_num_074',
                            split_separator=';',
                            join_separator=' OR '))

                    if gpo_item_num_074 != '':
                        search_query += f' AND {gpo_item_num_074}'

        assert search_query is not None, ('Cannot build a valid search query. '
            'Record from input file must include at least one of the following '
            'record identifiers: lccn_fixed (i.e. a corrected version of the '
            'lccn value), lccn, isbn, issn, gov_doc_class_num_086. These '
            'record identifiers are either empty or invalid.')

        # Build URL for API request
        url = (f"{os.environ['WORLDCAT_METADATA_API_URL_FOR_SEARCH']}"
            f"/brief-bibs"
            f"?q={search_query}"
            f"&limit=2")

        api_response = None
        json_response = None
        api_response_label = 'Search Brief Bibliographic Resources API response'

        try:
            num_records_held_by_your_library = None
            num_records_label = (f"records held by "
                f"{os.environ['OCLC_INSTITUTION_SYMBOL']}")
            num_records_total = None

            # Delete after testing (must add this as a function parameter)
            search_your_library_holdings_first = True

            if search_your_library_holdings_first:

                api_response, json_response = \
                    super().search_worldcat_and_log_response(
                        self.oauth_session.get,
                        (f"{url}&heldBySymbol="
                            f"{os.environ['OCLC_INSTITUTION_SYMBOL']}"),
                        (f"{api_response_label} ({num_records_label})"))

                num_records_held_by_your_library = self.get_num_records_dict(
                    json_response['numberOfRecords'],
                    records_label=num_records_label)

                # Delete after testing
                logger.info(f'{num_records_held_by_your_library = }')

                if num_records_held_by_your_library['value'] == 1:
                    # Found single WorldCat search result, so save OCLC Number
                    num_records_held_by_your_library['oclc_num'] = \
                        json_response['briefRecords'][0]['oclcNumber']

                    self.update_dataframe_for_input_file(
                        num_records_held_by_your_library)
                elif num_records_held_by_your_library['value'] > 1:
                    # Found multiple WorldCat search results held by your
                    # library
                    self.update_dataframe_for_input_file(
                        num_records_held_by_your_library)
                else:
                    # Found no WorldCat search results held by your library, so
                    # search WorldCat WITHOUT a "held by" filter.
                    logger.debug(f'Found no {num_records_label}. Searching '
                        f'without the "held by" filter...')

                    api_response = None
                    json_response = None

                    api_response, json_response = \
                        super().search_worldcat_and_log_response(
                            self.oauth_session.get,
                            url,
                            (f'{api_response_label} (all records; no "held by" '
                                f'filter)'))

                    num_records_total = self.get_num_records_dict(
                        json_response['numberOfRecords'])

                    # Delete after testing
                    logger.info(f'{num_records_total = }')

                    if num_records_total['value'] == 1:
                        # Found a single WorldCat search result, so save the
                        # OCLC Number
                        num_records_total['oclc_num'] = \
                            json_response['briefRecords'][0]['oclcNumber']

                        self.update_dataframe_for_input_file(num_records_total)
                    else:
                        # Found zero or multiple WorldCat search results
                        self.update_dataframe_for_input_file(num_records_total)
                        self.update_dataframe_for_input_file(
                            num_records_held_by_your_library)
        except json.decoder.JSONDecodeError:
        # except (requests.exceptions.JSONDecodeError,
        #         json.decoder.JSONDecodeError):
            logger.exception(f'Problem with {api_response_label}: Error '
                f'decoding JSON')
            logger.error(f'{api_response.text = }')

            # Re-raise exception so that the script is halted (since future API
            # requests may result in the same error)
            raise

        logger.debug('Finished processing records buffer.')

    def remove_all_records(self) -> None:
        """Removes all records from this buffer (i.e. clears record_list)."""

        self.record_list.clear()
        logger.debug(f'Cleared records buffer.')
        logger.debug(self.__str__() + '\n')

    def update_dataframe_for_input_file(
            self,
            num_records_dict: Dict[str, Union[int, str]]) -> None:
        """Updates the input file's DataFrame based on the given dictionary.

        Parameters
        ----------
        num_records_dict: Dict[str, Union[int, str]]
            The dictionary whose data will be used for the update
        """

        log_msg = f"For row {self.record_list[0].Index + 2}"

        if 'oclc_num' in num_records_dict:
            logger.debug(f"{log_msg}, the OCLC Number is "
                f"{num_records_dict['oclc_num']}")

            # Add OCLC Number to DataFrame
            self.dataframe_for_input_file.loc[
                self.record_list[0].Index,
                'oclc_num'
            ] = num_records_dict['oclc_num']
        else:
            logger.debug(f"{log_msg}, {num_records_dict['log_msg']}")

            # Add number of records found to DataFrame
            self.dataframe_for_input_file.loc[
                self.record_list[0].Index,
                num_records_dict['column_name']
            ] = num_records_dict['value']
