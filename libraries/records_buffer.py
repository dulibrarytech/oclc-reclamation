import calendar
import dotenv
import json
import libraries.api
import libraries.handle_file
import libraries.record
import libraries.xml
import logging
import os
import pandas as pd
import requests
import time
import xml.etree.ElementTree as ET
from csv import writer
from oauthlib.oauth2 import BackendApplicationClient, TokenExpiredError
from requests.auth import HTTPBasicAuth
from requests_oauthlib import OAuth2Session
from typing import (Any, Callable, Dict, List, NamedTuple, Optional, Set,
    TextIO, Tuple, Union)

dotenv_file = dotenv.find_dotenv()
dotenv.load_dotenv(dotenv_file)

logger = logging.getLogger(__name__)


class AlmaRecordsBuffer:
    """A buffer of Alma records.

    Use this class to update each Alma record. See update_alma_records.py for
    more details.

    Contains a dictionary that maps MMS ID to OCLC Number.

    Attributes
    ----------
    mms_id_to_oclc_num_dict: Dict[str, str]
        A dictionary mapping MMS ID (key) to OCLC Number (value)
    api_request_headers: Dict[str, str]
        The HTTP headers to use when making Alma API requests
    num_api_requests_made: int
        The total number of Alma API requests made using this records buffer
    num_api_requests_remaining: Optional[int]
        The number of Alma Daily API requests remaining
    num_records_updated: int
        The number of records successfully updated
    num_records_with_no_update_needed: int
        The number of records with no update needed
    num_records_with_errors: int
        The number of records where an error was encountered
    records_updated: TextIO
        The CSV file object where updated records are added
    records_updated_writer: writer
        The writer object used to write to the records_updated CSV file object
    records_with_no_update_needed: TextIO
        The CSV file object where records with no update needed are added
    records_with_no_update_needed_writer: writer
        The writer object used to write to the
        records_with_no_update_needed_writer CSV file object
    records_with_errors: TextIO
        The CSV file object where records are added if an error is encountered
    records_with_errors_writer: writer
        The writer object used to write to the records_with_errors CSV file
        object

    Methods
    -------
    add(mms_id, oclc_num)
        Adds the given record to this buffer (i.e. mms_id_to_oclc_num_dict)
    process_records()
        Updates each Alma record in buffer (if an update is needed)
    remove_all_records()
        Removes all records from this buffer
    update_alma_record(mms_id, alma_record)
        Updates the Alma record to have the given OCLC number (if needed)
    update_num_api_requests(num_api_requests_remaining)
        Updates the number of Alma API requests made/remaining. Only call this
        method after making an Alma API request.
    """

    def __init__(
            self,
            records_updated: TextIO,
            records_with_no_update_needed: TextIO,
            records_with_errors: TextIO) -> None:
        """Initializes an AlmaRecordsBuffer object.

        Parameters
        ----------
        records_updated: TextIO
            The CSV file object where updated records are added
        records_with_no_update_needed: TextIO
            The CSV file object where records with no update needed are added
        records_with_errors: TextIO
            The CSV file object where records are added if an error is
            encountered
        """

        self.mms_id_to_oclc_num_dict = {}

        self.api_request_headers = {
            'Authorization': f'apikey {os.environ["ALMA_API_KEY"]}'
        }
        self.num_api_requests_made = 0
        self.num_api_requests_remaining = None

        self.num_records_updated = 0
        self.num_records_with_no_update_needed = 0
        self.num_records_with_errors = 0

        self.records_updated = records_updated
        self.records_with_no_update_needed = records_with_no_update_needed
        self.records_with_errors = records_with_errors

        self.records_updated_writer = writer(self.records_updated)
        self.records_with_no_update_needed_writer = \
            writer(self.records_with_no_update_needed)
        self.records_with_errors_writer = writer(self.records_with_errors)

    def __len__(self) -> int:
        """Returns the number of records in this records buffer.

        Returns
        -------
        int
            The number of records in this records buffer
        """

        return len(self.mms_id_to_oclc_num_dict)

    def __str__(self) -> str:
        """Returns a string listing the contents of this records buffer.

        In specific, this method lists the contents of the MMS ID to OCLC Number
        dictionary (mms_id_to_oclc_num_dict).

        Returns
        -------
        str
            The contents of this records buffer
        """

        return (f'Records buffer contents ({{MMS ID: OCLC Number}}): '
            f'{self.mms_id_to_oclc_num_dict}')

    def add(self, mms_id: str, oclc_num: str) -> None:
        """Adds the given record to this buffer (i.e. mms_id_to_oclc_num_dict).

        Parameters
        ----------
        mms_id: str
            The record's MMS ID
        oclc_num: str
            The record's OCLC Number

        Raises
        ------
        AssertionError
            If the MMS ID is already in the buffer, i.e. mms_id_to_oclc_num_dict
        """

        assert mms_id not in self.mms_id_to_oclc_num_dict, (f'MMS ID {mms_id} '
            f'already exists in records buffer with OCLC Number '
            f'{self.mms_id_to_oclc_num_dict[mms_id]}')
        self.mms_id_to_oclc_num_dict[mms_id] = oclc_num
        logger.debug(f'Added {mms_id} to records buffer.')

    def process_records(self) -> None:
        """Updates each Alma record in buffer (if an update is needed).

        Sends a GET request to the Ex Libris Alma BIBs API:
        https://developers.exlibrisgroup.com/alma/apis/bibs/

        Raises
        ------
        requests.exceptions.HTTPError
            If the API request results in a 4XX client error or 5XX server error
            response
        """

        assert len(self.mms_id_to_oclc_num_dict) != 0, ('Cannot process '
            'records because records buffer is empty.')

        logger.debug('Started processing records buffer...\n')

        logger.info(f'{self.__str__()}') # delete after testing

        api_response = None
        try:
            params = {
                'view': 'full',
                'mms_id': ','.join(self.mms_id_to_oclc_num_dict.keys())
            }

            logger.debug(f'Making GET request for '
                f'{len(self.mms_id_to_oclc_num_dict)} Alma record(s)...')

            # Make GET request to retrieve all Alma records in buffer
            api_response = requests.get(
                f'{os.environ["ALMA_BIBS_API_URL"]}',
                params=params,
                headers=self.api_request_headers,
                timeout=45
            )
            self.update_num_api_requests(
                int(api_response.headers['X-Exl-Api-Remaining'])
            )
            libraries.api.log_response_and_raise_for_status(api_response)

            root = ET.fromstring(api_response.text)
            num_records = int(root.attrib['total_record_count'])

            logger.debug(f'The GET request retrieved {num_records} Alma '
                f'record(s).\n')

            # Loop through each Alma record (i.e. each 'bib' element)
            for record_index, bib_element in enumerate(root, start=1):
                mms_id = bib_element.find('mms_id').text

                logger.debug(f'Started processing MMS ID {mms_id} (record '
                    f'#{record_index} of {num_records} in buffer)...')

                xml_as_pretty_printed_bytes_obj = libraries.xml.prettify(
                    ET.tostring(bib_element, encoding='UTF-8')
                )
                # To also log the record's XML to the console, use the following
                # code instead:
                # xml_as_pretty_printed_bytes_obj = \
                #     libraries.xml.prettify_and_log_xml(
                #         ET.tostring(bib_element, encoding='UTF-8'),
                #         'Original record'
                #     )

                # Create XML file
                with open(
                        f'outputs/update_alma_records/xml/{mms_id}_original.xml',
                        'wb') as file:
                    file.write(xml_as_pretty_printed_bytes_obj)

                # Note: The update_alma_record() method returns a
                # Record_confirmation NamedTuple (see libraries/record.py) which
                # should adhere to the following rule:
                # - If the was_updated field is True, then the error_msg field
                # will be None.
                updated_record_confirmation = self.update_alma_record(
                    mms_id,
                    bib_element
                )

                logger.info(f'{updated_record_confirmation = }') # delete after testing

                if updated_record_confirmation.was_updated:
                    self.num_records_updated += 1

                    # Add record to records_updated spreadsheet
                    if self.records_updated.tell() == 0:
                        # Write header row
                        self.records_updated_writer.writerow([
                            'MMS ID',
                            (f'Original OCLC Number(s) '
                                f'[{libraries.record.subfield_a_disclaimer}]'),
                            'New OCLC Number'
                        ])

                    self.records_updated_writer.writerow([
                        mms_id,
                        updated_record_confirmation.orig_oclc_nums,
                        self.mms_id_to_oclc_num_dict[mms_id]
                    ])
                elif updated_record_confirmation.error_msg is None:
                    self.num_records_with_no_update_needed += 1

                    # Add record to records_with_no_update_needed spreadsheet
                    if self.records_with_no_update_needed.tell() == 0:
                        # Write header row
                        self.records_with_no_update_needed_writer.writerow([
                            'MMS ID',
                            'OCLC Number'
                        ])

                    self.records_with_no_update_needed_writer.writerow([
                        mms_id,
                        self.mms_id_to_oclc_num_dict[mms_id]
                    ])
                else:
                    self.num_records_with_errors += 1

                    logger.info('Error within process_records() method. Inside '
                        '"else" block because updated_record_confirmation.was_update is False and '
                        'updated_record_confirmation.error_msg is not None') # delete after testing

                    logger.info(f'{type(updated_record_confirmation.error_msg) = }') # delete after testing

                    # Add record to records_with_errors spreadsheet
                    if self.records_with_errors.tell() == 0:
                        # Write header row
                        self.records_with_errors_writer.writerow([
                            'MMS ID',
                            (f'OCLC Number(s) from Alma Record '
                                f'[{libraries.record.subfield_a_disclaimer}]'),
                            'Current OCLC Number',
                            'Error'
                        ])

                    self.records_with_errors_writer.writerow([
                        mms_id,
                        updated_record_confirmation.orig_oclc_nums
                            if updated_record_confirmation.orig_oclc_nums is not None
                            else '<record not fully checked>',
                        self.mms_id_to_oclc_num_dict.get(
                            mms_id,
                            '<error retrieving OCLC Number>'
                        ),
                        updated_record_confirmation.error_msg
                    ])

                logger.debug(f'Finished processing MMS ID {mms_id} (record '
                    f'#{record_index} of {num_records} in buffer).\n')
        except requests.exceptions.HTTPError:
            libraries.xml.prettify_and_log_xml(
                api_response.text,
                'Alma API response',
                logger.error
            )

            # Re-raise exception so that it can be handled by the main script
            # (which will include a more complete stack trace)
            raise

        logger.debug('Finished processing records buffer.\n')

    def remove_all_records(self) -> None:
        """Removes all records from this buffer.

        In specific, clears mms_id_to_oclc_num_dict.
        """

        self.mms_id_to_oclc_num_dict.clear()
        logger.debug(f'Cleared records buffer.')
        logger.debug(self.__str__() + '\n')

    def update_alma_record(
            self,
            mms_id: str,
            alma_record: ET.Element) -> libraries.record.Record_confirmation:
        """Updates the Alma record to have the given OCLC Number (if needed).

        Note that the OCLC Number is stored in mms_id_to_oclc_num_dict (with the
        Alma record's MMS ID as its key).

        Compares all 035 fields containing an OCLC number (in the subfield $a)
        to the given OCLC Number (oclc_num) in the records buffer (i.e. in
        mms_id_to_oclc_num_dict). If needed, updates the Alma record such that:
        - the record contains the given OCLC number (oclc_num)
        - any non-matching OCLC numbers from an 035 field (in the subfield $a)
          are moved to the 019 field (and that 035 field is removed, along with
          any data in its subfields)

        Updates the Alma record by sending a PUT request to the Ex Libris Alma
        BIBs API: https://developers.exlibrisgroup.com/alma/apis/bibs/

        Parameters
        ----------
        mms_id: str
            The MMS ID of the Alma record to be updated
        alma_record: ET.Element
            The Alma record's top-level element (i.e. bib element) from the
            parsed XML tree

        Returns
        -------
        libraries.record.Record_confirmation
            NamedTuple with details about the update attempt. Includes the
            following fields:
            - was_updated
            - orig_oclc_nums
            - error_msg

        Raises
        ------
        requests.exceptions.HTTPError
            If the API request results in a 4XX client error or 5XX server error
            response
        """

        logger.debug(f"Attempting to update MMS ID '{mms_id}'...")

        oclc_num = self.mms_id_to_oclc_num_dict[mms_id]
        full_oclc_num = f'{libraries.record.oclc_org_code_prefix}{oclc_num}'
        logger.debug(f'Full OCLC number: {full_oclc_num}')

        # Access XML elements of Alma record
        record_element = alma_record.find('./record')
        need_to_update_record = False
        oclc_nums_from_record = list()
        oclc_nums_for_019_field = set()
        found_035_field_with_current_oclc_num = False
        record_contains_potentially_valid_oclc_num_with_invalid_oclc_prefix = False
        error_msg = None
        found_error_in_record = False

        # Iterate over each 035 field
        for field_035_element_index, field_035_element in enumerate(
                record_element.findall('./datafield[@tag="035"]')):
            # Extract subfield a (which would contain the OCLC number if present)
            subfield_a_data = libraries.record.get_subfield_a_with_oclc_num(
                field_035_element,
                field_035_element_index)

            # Add or append to error message
            if subfield_a_data.error_msg is not None:
                if error_msg is None:
                    error_msg = subfield_a_data.error_msg
                else:
                    error_msg += '. ' + subfield_a_data.error_msg

            if subfield_a_data.string_with_oclc_num is None:
                # This 035 field either has no subfield $a or its first subfield $a
                # does not contain an OCLC number. So skip it.
                continue

            (subfield_a_without_oclc_org_code_prefix,
                    extracted_oclc_num,
                    found_valid_oclc_prefix,
                    found_valid_oclc_num,
                    found_error_in_record) = \
                libraries.record.extract_oclc_num_from_subfield_a(
                    subfield_a_data.string_with_oclc_num,
                    field_035_element_index,
                    found_error_in_record)

            oclc_nums_from_record.append(subfield_a_without_oclc_org_code_prefix)

            # Check for potentially-valid OCLC number with invalid prefix
            found_potentially_valid_oclc_num_with_invalid_oclc_prefix = \
                found_valid_oclc_num and not found_valid_oclc_prefix

            if found_potentially_valid_oclc_num_with_invalid_oclc_prefix:
                invalid_prefix_msg = (f'035 field #{field_035_element_index + 1} '
                    f'contains an OCLC number with an invalid prefix: '
                    f'{extracted_oclc_num}. '
                    f'{libraries.record.valid_oclc_number_prefixes_str}')
                if error_msg is None:
                    error_msg = invalid_prefix_msg
                else:
                    error_msg += '. ' + invalid_prefix_msg

            record_contains_potentially_valid_oclc_num_with_invalid_oclc_prefix = \
                (record_contains_potentially_valid_oclc_num_with_invalid_oclc_prefix
                 or found_potentially_valid_oclc_num_with_invalid_oclc_prefix)

            if not record_contains_potentially_valid_oclc_num_with_invalid_oclc_prefix:
                # Compare the extracted OCLC number to the current OCLC number
                extracted_oclc_num_matches_current_oclc_num = \
                    extracted_oclc_num == oclc_num
                logger.debug(f'Does the extracted OCLC number '
                    f'({extracted_oclc_num}) match the current OCLC number '
                    f'({oclc_num})? {extracted_oclc_num_matches_current_oclc_num}')

                if (not extracted_oclc_num_matches_current_oclc_num
                        or found_035_field_with_current_oclc_num):
                    # This 035 field either (1) contains an old, empty or invalid
                    # OCLC number or (2) is a duplicate of another 035 field with
                    # the current OCLC number. In either case, remove this 035
                    # field.
                    record_element.remove(field_035_element)
                    logger.debug(f'Removed 035 field #'
                        f'{field_035_element_index + 1}, whose (first) subfield a '
                        f'is: {subfield_a_data.string_with_oclc_num}')

                    if (not extracted_oclc_num_matches_current_oclc_num
                            and len(extracted_oclc_num) > 0
                            and found_valid_oclc_num):
                        oclc_nums_for_019_field.add(extracted_oclc_num)

                    need_to_update_record = True
                else:
                    found_035_field_with_current_oclc_num = True

        logger.debug(f'{oclc_nums_for_019_field=}')
        logger.debug(f'{len(oclc_nums_for_019_field)=}')

        oclc_nums_from_record_list_length = len(oclc_nums_from_record)
        oclc_nums_from_record_str = None
        if oclc_nums_from_record_list_length > 0:
            oclc_nums_from_record_str = ', '.join(oclc_nums_from_record)

        logger.debug(f'{oclc_nums_from_record=}')
        logger.debug(f'{oclc_nums_from_record_list_length=}')
        logger.debug(f'{oclc_nums_from_record_str=}')

        # Don't update the record if it contains a potentially-valid OCLC number
        # with an invalid prefix.
        if record_contains_potentially_valid_oclc_num_with_invalid_oclc_prefix:
            logger.error(f"Did not update MMS ID '{mms_id}' because it contains at "
                f"least one potentially-valid OCLC number with an invalid prefix."
                f"\n")

            return libraries.record.Record_confirmation(
                False,
                oclc_nums_from_record_str,
                error_msg
            )

        # Only add or edit the 019 field if oclc_nums_for_019_field set is non-empty
        if oclc_nums_for_019_field:
            # Search record for 019 field
            first_019_element = record_element.find('./datafield[@tag="019"]')

            # If the record has no 019 field, create one
            if not first_019_element:
                logger.debug(f'Original record does not have an 019 field.')
                first_019_element = ET.SubElement(record_element, 'datafield')
                first_019_element.set('ind1', ' ')
                first_019_element.set('ind2', ' ')
                first_019_element.set('tag', '019')

            first_019_element_as_str = \
                ET.tostring(first_019_element, encoding="unicode")
            logger.debug(f'First 019 element:\n{first_019_element_as_str}')

            # Add old OCLC numbers to 019 field
            for old_oclc_num in oclc_nums_for_019_field:
                sub_element = ET.SubElement(first_019_element, 'subfield')
                sub_element.set('code', 'a')
                sub_element.text = old_oclc_num

                first_019_element_as_str = \
                    ET.tostring(first_019_element, encoding="unicode")
                logger.debug(f'First 019 element after adding {old_oclc_num}:\n'
                    f'{first_019_element_as_str}')

        if not found_035_field_with_current_oclc_num:
            # Create new 035 element with OCLC number
            new_035_element = ET.SubElement(record_element, 'datafield')
            new_035_element.set('ind1', ' ')
            new_035_element.set('ind2', ' ')
            new_035_element.set('tag', '035')
            sub_element = ET.SubElement(new_035_element, 'subfield')
            sub_element.set('code', 'a')
            sub_element.text = full_oclc_num

            new_035_element_as_str = \
                ET.tostring(new_035_element, encoding="unicode")
            logger.debug(f'New 035 element:\n{new_035_element_as_str}')

            need_to_update_record = True

        if need_to_update_record:
            api_response = None
            try:
                headers = {
                    'Authorization': self.api_request_headers['Authorization'],
                    'Content-Type': 'application/xml'
                }
                payload = ET.tostring(alma_record, encoding='UTF-8')

                # # delete after testing (entire 'if' block)
                # # Code for testing how update_alma_record() method handles an HTTP Error
                # if mms_id == '991027570199702766':
                #     logger.info(f'{mms_id = }')
                #     mms_id = '12345'
                #     logger.info(f'Changed mms_id to {mms_id}')

                # Make PUT request to update Alma record
                api_response = requests.put(
                    f'{os.environ["ALMA_BIBS_API_URL"]}{mms_id}',
                    headers=headers,
                    data=payload,
                    timeout=45
                )
                self.update_num_api_requests(
                    int(api_response.headers['X-Exl-Api-Remaining'])
                )
                libraries.api.log_response_and_raise_for_status(api_response)

                xml_as_pretty_printed_bytes_obj = \
                    libraries.xml.prettify(api_response.text)
                # To also log the updated record's XML to the console, use the
                # following code instead:
                # xml_as_pretty_printed_bytes_obj = \
                #     libraries.xml.prettify_and_log_xml(
                #         api_response.text,
                #         'Modified record'
                #     )

                # Create XML file
                with open(f'outputs/update_alma_records/xml/{mms_id}_modified.xml',
                        'wb') as file:
                    file.write(xml_as_pretty_printed_bytes_obj)

                logger.debug(f"MMS ID '{mms_id}' has been updated.\n")
                return libraries.record.Record_confirmation(
                    True,
                    oclc_nums_from_record_str,
                    None
                )
            except requests.exceptions.HTTPError as http_err:
                libraries.xml.prettify_and_log_xml(
                    api_response.text,
                    'Alma API response',
                    logger.error
                )

                logger.exception(f"Error attempting to update MMS ID "
                    f"'{mms_id}'.\n")
                logger.debug('Logged exception (which should include stack trace). Now returning from update_alma_record() method...') # delete after testing
                return libraries.record.Record_confirmation(
                    False,
                    oclc_nums_from_record_str,
                    (f'Error attempting to update Alma record: HTTP Error: '
                        f'{http_err}')
                )

        logger.debug(f"No update needed for MMS ID '{mms_id}'.\n")
        return libraries.record.Record_confirmation(
            False,
            oclc_nums_from_record_str,
            None
        )

    def update_num_api_requests(
            self,
            num_api_requests_remaining: int) -> None:
        """Updates the number of Alma API requests made/remaining.

        Only call this method after making an Alma API request.

        Parameters
        ----------
        num_api_requests_remaining: int
            The number of daily Ex Libris API requests remaining
        """

        self.num_api_requests_made += 1
        self.num_api_requests_remaining = num_api_requests_remaining

        logger.debug(f'After API request, there are '
            f'{self.num_api_requests_remaining} Ex Libris API requests '
            f'remaining for today.')


class WorldCatRecordsBuffer:
    """
    A buffer of records for WorldCat. DO NOT INSTANTIATE THIS CLASS DIRECTLY.

    Instead, instantiate one of its subclasses:
    - OclcNumDictBuffer: A buffer containing a dictionary that maps OCLC Number
      (key) to MMS ID (value). Use this subclass to find each record's current
      OCLC Number. See process_worldcat_records.py for more details.
    - OclcNumSetBuffer: A buffer containing a set of OCLC Numbers. Use this
      subclass to set or unset the WorldCat holding for each OCLC Number. See
      process_worldcat_records.py for more details.
    - RecordSearchBuffer: A buffer containing data to be searched for in
      WorldCat. Use this subclass to find a record's OCLC Number given other
      record identifiers. See search_worldcat.py for more details.

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
    make_api_request_and_log_response(api_request, api_url, api_response_label)
        Makes the specified API request and logs the response
    """

    def __init__(self) -> None:
        """Initializes a WorldCatRecordsBuffer object."""

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

    def make_api_request_and_log_response(
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


class OclcNumDictBuffer(WorldCatRecordsBuffer):
    """A buffer containing a dictionary mapping OCLC Number to MMS ID.

    Use this subclass to find each record's current OCLC Number. See
    process_worldcat_records.py for more details.

    Attributes
    ----------
    oclc_num_dict: Dict[str, str]
        A dictionary that maps each record's original OCLC Number (key) to its
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
        """Instantiates an OclcNumDictBuffer object.

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
            api_response, json_response = \
                super().make_api_request_and_log_response(
                    self.oauth_session.get,
                    url,
                    'Get Current OCLC Number API response'
                )

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


class OclcNumSetBuffer(WorldCatRecordsBuffer):
    """
    A buffer containing a set of OCLC Numbers.

    Use this subclass to set or unset the WorldCat holding for each OCLC Number.
    See process_worldcat_records.py for more details.

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
        """Instantiates an OclcNumSetBuffer object.

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

        # Build URL for API request
        url = (f"{os.environ['WORLDCAT_METADATA_API_URL']}"
            f"/ih/datalist?oclcNumbers={','.join(self.oclc_num_set)}")

        api_name = None
        api_request = None
        if self.set_or_unset_choice == 'set':
            api_name = 'Set Holding API'
            api_request = self.oauth_session.post
        else:
            api_name = 'Unset Holding API'
            api_request = self.oauth_session.delete

            # Include "cascade" URL parameter for unset_holding operation
            url += f'&cascade={self.cascade}'

        api_response_error_msg = f'Problem with {api_name} response'

        try:
            api_response, json_response = \
                super().make_api_request_and_log_response(
                    api_request,
                    url,
                    f'{api_name} response'
                )

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


class RecordSearchBuffer(WorldCatRecordsBuffer):
    """
    A buffer containing data to be searched for in WorldCat.

    Use this subclass to find a record's OCLC Number given other record
    identifiers. See search_worldcat.py for more details.

    This buffer must contain only one record at a time.

    Attributes
    ----------
    dataframe_for_input_file: pd.DataFrame
        The pandas DataFrame created from the input file
    num_records_needing_one_api_request: int
        The number of Alma records that needed a single WorldCat API request
    num_records_needing_two_api_requests: int
        The number of Alma records that needed two WorldCat API requests
    record_list: List[NamedTuple]
        A list containing the record data to use when searching WorldCat; this
        list should contain no more than one element (i.e. record)

    Methods
    -------
    add(record_data)
        Adds the given record to this buffer (i.e. to record_list)
    get_num_records_dict(num_records, used_held_by_filter)
        Creates a dictionary with data about the WorldCat search results
    process_records()
        Searches WorldCat using the record data in record_list
    remove_all_records()
        Removes all records from this buffer (i.e. clears record_list)
    update_dataframe_for_input_file(num_records_dict)
        Updates the input file's DataFrame (i.e. the dataframe_for_input_file
        attribute) based on the given dictionary
    """

    def __init__(self, dataframe_for_input_file: pd.DataFrame) -> None:
        """Instantiates a RecordSearchBuffer object.

        Parameters
        ----------
        dataframe_for_input_file: pd.DataFrame
            The pandas DataFrame created from the input file
        """

        self.record_list = []
        self.dataframe_for_input_file = dataframe_for_input_file
        self.num_records_needing_one_api_request = 0
        self.num_records_needing_two_api_requests = 0

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
            used_held_by_filter: bool = False) -> Dict[str, Union[int, str]]:
        """Creates a dictionary with data about the WorldCat search results.

        Parameters
        ----------
        num_records: int
            The number of records returned by the WorldCat search
        used_held_by_filter: bool, default is False
            Whether the WorldCat search used a "held by" filter (to limit
            the results to your library's holdings only)

        Returns
        -------
        Dict[str, Union[int, str]]
            The dictionary created based on the given WorldCat search data
        """

        column_name = None
        log_msg = None

        if used_held_by_filter:
            column_name = (f"num_records_held_by_"
                f"{os.environ['OCLC_INSTITUTION_SYMBOL']}")
            log_msg = (f"found {num_records} records held by "
                f"{os.environ['OCLC_INSTITUTION_SYMBOL']}")
        else:
            column_name = "num_records_total"
            log_msg = f"found {num_records} total records"

        return {
            'value': num_records,
            'column_name': column_name,
            'log_msg': log_msg
        }

    def process_records(
            self,
            search_my_library_holdings_first: bool = False) -> None:
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

        Makes up to two searches:
        - with "held by" filter (search your library's holdings only)
        - without "held by" filter (search all Worldat records)

        Parameters
        ----------
        search_my_library_holdings_first: bool, default is False
            Whether to first search WorldCat for your library's holdings only.
            - True:
                1) Search with "held by" filter.
                2) If there are no WorldCat search results held by your library,
                   then search without "held by" filter.
            - False:
                1) Search without "held by" filter.
                2) If there is more than one WorldCat search result, then
                   search with "held by" filter.

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
            num_api_requests_made_before_current_search = \
                self.num_api_requests_made

            if search_my_library_holdings_first:
                api_response, json_response = \
                    super().make_api_request_and_log_response(
                        self.oauth_session.get,
                        (f"{url}&heldBySymbol="
                            f"{os.environ['OCLC_INSTITUTION_SYMBOL']}"),
                        (f"{api_response_label} ({num_records_label})"))

                num_records_held_by_your_library = self.get_num_records_dict(
                    json_response['numberOfRecords'],
                    used_held_by_filter=True)

                if num_records_held_by_your_library['value'] > 0:
                    if num_records_held_by_your_library['value'] == 1:
                        # Found a single WorldCat search result, so save the
                        # OCLC Number
                        num_records_held_by_your_library['oclc_num'] = \
                            json_response['briefRecords'][0]['oclcNumber']

                    self.update_dataframe_for_input_file(
                        num_records_held_by_your_library)
                else:
                    # Found no WorldCat search results held by your library, so
                    # search WorldCat WITHOUT the "held by" filter
                    logger.debug(f'Found no {num_records_label}. Searching '
                        f'without the "held by" filter...')

                    api_response = None
                    json_response = None

                    api_response, json_response = \
                        super().make_api_request_and_log_response(
                            self.oauth_session.get,
                            url,
                            (f'{api_response_label} (all records; no "held by" '
                                f'filter)'))

                    num_records_total = self.get_num_records_dict(
                        json_response['numberOfRecords'])

                    if num_records_total['value'] == 1:
                        # Found a single WorldCat search result, so save the
                        # OCLC Number
                        num_records_total['oclc_num'] = \
                            json_response['briefRecords'][0]['oclcNumber']
                    else:
                        # Found zero or multiple WorldCat search results
                        self.update_dataframe_for_input_file(
                            num_records_held_by_your_library)

                    # Either way, update the input file's DataFrame with data
                    # from num_records_total
                    self.update_dataframe_for_input_file(num_records_total)
            else:
                # First search WITHOUT "held by" filter
                api_response, json_response = \
                    super().make_api_request_and_log_response(
                        self.oauth_session.get,
                        url,
                        (f'{api_response_label} (all records; no "held by" '
                            f'filter)'))

                num_records_total = self.get_num_records_dict(
                    json_response['numberOfRecords'])

                if num_records_total['value'] <= 1:
                    if num_records_total['value'] == 1:
                        # Found a single WorldCat search result, so save the
                        # OCLC Number
                        num_records_total['oclc_num'] = \
                            json_response['briefRecords'][0]['oclcNumber']

                    self.update_dataframe_for_input_file(num_records_total)
                else:
                    # Found multiple WorldCat search results, so search WorldCat
                    # WITH a "held by" filter
                    logger.debug(f"Found {num_records_total['value']} total "
                        f'records. Searching with a "held by" filter to '
                        f'narrow down the results...')

                    api_response = None
                    json_response = None

                    api_response, json_response = \
                        super().make_api_request_and_log_response(
                            self.oauth_session.get,
                            (f"{url}&heldBySymbol="
                                f"{os.environ['OCLC_INSTITUTION_SYMBOL']}"),
                            (f"{api_response_label} ({num_records_label})"))

                    num_records_held_by_your_library = \
                        self.get_num_records_dict(
                            json_response['numberOfRecords'],
                            used_held_by_filter=True)

                    if num_records_held_by_your_library['value'] == 1:
                        # Found a single WorldCat search result, so save the
                        # OCLC Number
                        num_records_held_by_your_library['oclc_num'] = \
                            json_response['briefRecords'][0]['oclcNumber']
                    else:
                        # Found zero or multiple WorldCat search results
                        self.update_dataframe_for_input_file(num_records_total)

                    # Either way, update the input file's DataFrame with data
                    # from num_records_held_by_your_library
                    self.update_dataframe_for_input_file(
                        num_records_held_by_your_library)

            num_api_requests_made_during_current_search = (
                self.num_api_requests_made
                    - num_api_requests_made_before_current_search)

            if num_api_requests_made_during_current_search == 1:
                self.num_records_needing_one_api_request += 1
            elif num_api_requests_made_during_current_search == 2:
                self.num_records_needing_two_api_requests += 1
            else:
                logger.warning(f'For row {self.record_list[0].Index + 2}, '
                    f'{num_api_requests_made_during_current_search} API '
                    f'requests were made when searching WorldCat. The number '
                    f'of API requests per row should be either 1 or 2.')
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
